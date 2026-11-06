"""MCP server exposing the published Spicy Regs tables as read-only SQL tools.

The tools (``list_sources``, ``describe_table``, ``query_sql``) run over a
cached DuckDB connection whose views are pinned to one publication snapshot,
reading either the public R2 bucket or an explicitly configured local directory
(``SPICY_REGS_DATA_DIR``). The HTTP app also serves the human setup page at ``/``.
"""

from __future__ import annotations

from spicy_regs.subject_catalog import subject_tables

import base64
import difflib
import functools
import hashlib
import inspect
import json
import logging
import os
import tempfile
import threading
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from time import monotonic as _monotonic
from typing import Annotated, Any, NamedTuple
from urllib.parse import urlsplit
from uuid import UUID

import anyio
import anyio.to_thread
import duckdb
import httpx
from spicy_regs.connection_diagnostics import phase as _diagnostic_phase, snapshot as _diagnostic_snapshot
import pydantic_core
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.tools import Tool
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, Icon, TextContent
from pydantic import Field, ValidationError
from starlette.requests import Request
from starlette.responses import Response

from spicy_regs import receipt_lookup
from spicy_regs.runtime_bounds import RequestBound, bounded_connection, checkpoint, current_bound
from spicy_regs._icon import ICON_DATA_URI
from spicy_regs.citation_resolution import SOURCE_TABLES
from spicy_regs.duckdb_settings import INTERACTIVE_HTTP_RETRIES, load_public_http, memory_limit
from spicy_regs.fec_release import QUERY_RELEASE_FIELDS, RELEASE_INVENTORIES, release_summary
from spicy_regs.public_url import resolve_r2_base_url, service_url
from spicy_regs.fec_receipt_adapter import qualified_views, selected_qualified_views
from spicy_regs.vocabulary_mapping import Namespace

TABLES = (
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
    # The materialized rulemaking dataset, read through its own snapshot pointer
    # (publication.SNAPSHOT_POINTER), not the publication index.
    "rule_targets",
    "proceedings",
    "regulatory_agenda_items",
    "agenda_item_proceedings",
    "comment_periods",
    "rulemaking_lifecycles",
    "lifecycle_events",
    "agency_lifecycle_stats",
    "sam_entities",
    "cms_part_d_drug_spending",
    "cms_medicaid_drug_spending",
    "cms_part_b_drug_spending",
    "grants_opportunity_observations",
    "ofac_entity_observations",
    "ofac_snapshots",
    "executive_order_observations",
    "fec_financial_profiles",
    "treasury_debt_observations",
    "treasury_statement_periods",
    "treasury_statement_classification_cells",
    "census_acs_county_observations",
    "lobbying_filings",
    "lobbying_activities",
    "lobbying_activity_lobbyists",
    "lobbying_foreign_entities",
    "lobbying_affiliated_organizations",
    "lobbying_conviction_disclosures",
    "lobbying_registrants",
    "lobbying_clients",
    "fec_committees",
    "fec_committee_history",
    "fec_candidate_history",
    "document_attributes",
    "docket_attributes",
    "comment_attributes",
    "fec_source_catalog",
    "fec_collections",
    "fec_source_records",
    "fec_relationships",
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
    "org_committee_links",
    "gao_reports",
    "gao_decisions",
    "gao_recommendations",
    "crs_reports",
    "court_dockets",
    "court_docket_groups",
    "court_opinion_clusters",
    "court_citations",
    "court_citation_map",
    "court_parentheticals",
    "court_opinions",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
    # The BillTrax-derived tables hosted from spicy-docs' table contracts.
    # Listed literally rather than imported from data_dictionary: that module
    # reads the contracts out of the spicy-docs wheel, and the MCP server is a
    # base install that must not require the source-readers group.
    # test_mcp_server_tables_match_dictionary keeps the two lists equal.
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
    "member_vote_terms",
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
    "scorecard_member_links",
    "scorecard_item_links",
    "committee_reports",
    "committee_report_cbo_letters",
    "report_sections",
    "hearing_transcripts",
    "hearing_bill_links",
    "cbo_cost_estimates",
    "cbo_feed_items",
    "house_activity_reports",
    "budget_volumes",
    "bill_committee_actions",
    "document_citations",
    "senate_expenditures",
    # A8/A9 (laws and rosters): the laws and committee-rosters rollups.
    "laws",
    "law_sections",
    "law_code_sections",
    "table3_records",
    "committees",
    "committee_assignments",
    # The Congress.gov index tables (gaps A5, A7, A10).
    "house_communications",
    "committee_meetings",
    "record_issues",
    "treaties",
    "nominations",
    # Processing state, not a contract table: the bill-family rollup retains one
    # BILLSTATUS folder listing entry per folder so the next run can prove a zip
    # unchanged without downloading it. Queryable for the same reason every
    # other published object is — it is on the public bucket either way, and a
    # reader asking "when did this pipeline last see the H.R. zip" should not
    # have to reach past the server to answer it.
    "bill_family_archives",
    "bill_vote_references",
    # The pre-BILLSTATUS backfill's retained state, published for the same
    # reason: it is on the bucket either way, and "which bills has the
    # backfill filled or refused, under what list stamp" is answerable
    # without reaching past the server.
    "bill_family_backfills",
    "bill_family_backfill_walks",
    "committee_report_reads",
    "document_citation_reads",
    "native_legal_references",
    "native_legal_reference_reads",
    "court_opinion_pdf_extractions",
)
TABLES = subject_tables(TABLES)

STATEMENT_TIMEOUT = os.environ.get("SPICY_REGS_STATEMENT_TIMEOUT", "790s")
#: The kinds resolve_document_citations accepts, and its schema lists with their tables: the kinds a writer emits.
DOCUMENT_KINDS = tuple(sorted(SOURCE_TABLES))
#: resolve_document_citations' page size: offset and cite_kind page a long document (round 5 measured a 500-row
#: hrpt964 reply at 283,615 characters, 64% of it fields that cannot vary within the document or the kind).
DEFAULT_OCCURRENCES, MAX_OCCURRENCES = 25, 100
#: The occurrence fields a citation reply states once (coordinator answer 6: a fixed list, so an occurrence has
#: the same fields on every page): those fixed for a document's text, and those fixed for a cite_kind's route and
#: rule. Round 5 measured them at 64% of a 500-row CRPT-118hrpt964 reply.
OCCURRENCE_DOCUMENT_FIELDS = ("document_kind", "document_key", "text_sha256", "body_rendition", "body_derivation",
                              "source_status", "resolution_rule")
OCCURRENCE_KIND_FIELDS = ("rule_name", "rule_version", "target_table", "target_snapshot", "target_table_selected",
                          "target_grain", "expected_cardinality")
#: What each target_status says. A citation reply defines the words its occurrences use, and only those: the five
#: definitions did not fit the tool text beside everything else it must say (round 6, phase3-review item 2).
TARGET_STATUS_MEANINGS = {
    "found": "The selected target table holds the cited key: one row, or at least one where target_grain names "
             "several rows per key.",
    "missing": "The selected target table holds no row with this key. The table may not cover the cited range "
               "(describe_table states its coverage), so this does not prove the target does not exist.",
    "ambiguous": "More than one target row has this key where one was expected; candidate_keys lists them.",
    "not_checked": "No lookup was made; reason says why (a key the citation did not settle, a source text not read, "
                   "no pinned target table, a limit, or a failed read).",
    "unsupported": "No route looks up this citation kind: it is held, never checked.",
}
#: An occurrence field that restates another field of the same occurrence; a reply drops it where they are equal.
#: target_rule names the kind on every row of 11 of the 13 held kinds (round 6, live document_citations).
OCCURRENCE_SAME_AS = {"target_kind": "cite_kind", "normalized_key": "target_key", "target_rule": "cite_kind"}
#: The acquisition queue's fields fixed for the reply on each item, and for a target kind's route.
QUEUE_ITEM_FIELDS = ("intended_query", "queue_rule", "acquisition_outcome", "retained")
QUEUE_KIND_FIELDS = ("target_snapshot", "provider_route", "status")
#: document_kind's schema description, derived from SOURCE_TABLES so a new kind cannot drift from its table.
DOCUMENT_KIND_TABLES = "Each kind's table: " + "; ".join(
    f"{kind}: {table}" for kind, table in sorted(SOURCE_TABLES.items())
)

logger = logging.getLogger(__name__)

DEFAULT_CATALOG_NAMESPACE = "default"
FEC_QUERY_SCOPE = json.loads(files("spicy_regs").joinpath("fec_query_scope.json").read_text(encoding="utf-8"))
FEC_QUALIFIED_VIEWS: tuple = qualified_views(FEC_QUERY_SCOPE)
FEC_LEGACY_QUALIFIED_VIEWS: tuple = qualified_views(FEC_QUERY_SCOPE, legacy_associations=True)


def _parse_timeout_seconds(raw: str) -> float | None:
    """Parse a ``ms``/``s``/``m`` duration; an invalid spelling raises RuntimeError, non-positive means no timeout."""
    text = raw.strip().lower()
    if not text:
        return None
    multiplier = 1.0
    for suffix, factor in (("ms", 0.001), ("s", 1.0), ("m", 60.0)):
        if text.endswith(suffix):
            multiplier = factor
            text = text[: -len(suffix)].strip()
            break
    try:
        value = float(text)
    except ValueError as exc:
        raise RuntimeError(f"SPICY_REGS_STATEMENT_TIMEOUT is not a valid duration: {raw!r}") from exc
    if value <= 0:
        return None
    return value * multiplier


STATEMENT_TIMEOUT_SECONDS = _parse_timeout_seconds(STATEMENT_TIMEOUT)

INSTRUCTIONS = (
    "Query Spicy Regs public datasets across government and attributed third-party sources. "
    "With no subject, list_sources shows default table lists and an unavailable preview. "
    "Both omit a designated set of intentionally empty tables and sample-sized datasets. "
    "Use list_sources(subject) for all loaded tables in a subject, with labels, coverage and row counts. "
    "Use list_sources('views') for the full unavailable list. Use describe_table for actual "
    "schemas, field meanings, identifiers and coverage caveats, and query_sql for "
    "read-only queries and joins. A declared output or coverage measurement does "
    "not establish publication or freshness. qualification reports the output "
    "ledger's audit disposition for its own pin beside the live pin; it is not a "
    "verification flag for the live generation. Always LIMIT exploratory results. "
    "Cite source identifiers, evidence locators and dates from returned rows. "
    "When reporting scorecard ratings or preferred actions, name the publisher, edition and metric, "
    "preserve the literal value, and distinguish publisher observations from official congressional records. "
    "Use the native keys in describe_table's canonical joins. Before joining scorecard analysis to source rows, "
    "verify the analysis generation's source parent matches the selected scorecards generation. "
    "If compatibility is unavailable or differs, do not present those links as current. "
    "Derived relationship views retain source occurrences separately from distinct pairs. "
    "Use resolve_document_citations for bounded target lookups; a normalized citation key alone does not prove existence."
)

ICONS = [Icon(src=ICON_DATA_URI, mime_type="image/png", sizes=["512x512"])]


def _resolve_r2_base_url() -> str:
    return resolve_r2_base_url()


R2_BASE_URL = _resolve_r2_base_url()


def _resolve_data_dir() -> Path | None:
    """An explicit local source replaces remote tables for the whole connection."""
    raw = os.environ.get("SPICY_REGS_DATA_DIR")
    if raw is None:
        return None
    if not raw.strip():
        raise RuntimeError("SPICY_REGS_DATA_DIR must name a directory")
    # Resolve a `current` symlink when building each connection, not at import.
    path = Path(raw).expanduser().absolute()
    if not path.is_dir():
        raise RuntimeError(f"SPICY_REGS_DATA_DIR is not a directory: {path}")
    return path


DATA_DIR = _resolve_data_dir()


def _source_details(cursor: duckdb.DuckDBPyConnection | None = None) -> dict[str, str]:
    """Describe the active data source (local directory or R2) for tool replies."""
    if DATA_DIR is not None:
        selection = _connection_local_selection(cursor) if cursor is not None else None
        if selection is not None:
            return {"source": "local", "base_path": str(DATA_DIR), "selected_directory": selection["directory"]}
        return {"source": "local", "base_path": str(DATA_DIR)}
    return {"source": "r2", "base_url": R2_BASE_URL}


def _resolve_home_directory() -> str:
    """DuckDB home directory from SPICY_REGS_HOME_DIR (default system temp); rejects control characters."""
    raw = os.environ.get("SPICY_REGS_HOME_DIR", tempfile.gettempdir())
    if any(c in raw for c in ("\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_HOME_DIR contains illegal characters: {raw!r}")
    return raw


HOME_DIRECTORY = _resolve_home_directory()


def _resolve_memory_limit() -> str | None:
    """DuckDB memory ceiling from SPICY_REGS_MEMORY_LIMIT (e.g. '12GB', '16GiB').

    Unset => None => DuckDB's own default (~80% of detected RAM). Set it on hosts
    where DuckDB can't see the real allocation (containers detect host RAM, not
    the cgroup limit) so it spills/errors before the platform OOM-kills the process.
    Interpolated into a SET, so the value is format-validated.
    """
    raw = os.environ.get("SPICY_REGS_MEMORY_LIMIT", "").strip()
    if not raw:
        return None
    return memory_limit(raw, "SPICY_REGS_MEMORY_LIMIT")


def _resolve_temp_dir() -> str:
    """DuckDB spill directory from SPICY_REGS_TEMP_DIR.

    Default '' disables spilling — the safe serverless behavior, since DuckDB's
    default temp dir is a relative '.tmp' that is read-only on serverless hosts,
    so a spilling query would fail there anyway. Supply a writable path (a
    container with real disk, or a mounted volume) to let big GROUP BY/ORDER BY
    spill instead of erroring. Interpolated into a SET, so injection chars are
    rejected.
    """
    raw = os.environ.get("SPICY_REGS_TEMP_DIR", "")
    if any(c in raw for c in ("'", "\\", "\x00", "\n", "\r")):
        raise RuntimeError(f"SPICY_REGS_TEMP_DIR contains illegal characters: {raw!r}")
    return raw


def _resolve_tool_concurrency() -> int:
    """How many tool calls run at once, from SPICY_REGS_TOOL_CONCURRENCY (default 2); each has a thread and cursor.

    A remote scan issues range requests from every DuckDB thread: one persona query peaked at 72 requests/s on
    four threads (2026-09-28), and r2.dev throttles at hundreds per second across all of the bucket's readers.
    Two calls stay near that single-query rate. Raise it once the bucket is served from a custom domain.
    """
    raw = os.environ.get("SPICY_REGS_TOOL_CONCURRENCY", "2").strip()
    if not raw.isdecimal() or int(raw) < 1:
        raise RuntimeError(f"SPICY_REGS_TOOL_CONCURRENCY must be a positive integer: {raw!r}")
    return int(raw)


def _resolve_reply_chars() -> int:
    """The most characters one query or citation reply may carry, from SPICY_REGS_REPLY_CHARS (default 40,000).

    Claude Code saves a tool result past 25,000 tokens (``MAX_MCP_OUTPUT_TOKENS``) to a one-line file and its reader
    then showed 39,000 to 43,000 characters of each (round 6: replies of 76,947 to 102,942 characters). A reply that
    would pass the budget is refused with how much fits and how to ask for it (owner decision 2026-10-03); nothing
    partial is returned for size. Set by the environment only: an argument would spend every tool's description cap.
    """
    raw = os.environ.get("SPICY_REGS_REPLY_CHARS", "40000").strip()
    if not raw.isdecimal() or int(raw) < 1:
        raise RuntimeError(f"SPICY_REGS_REPLY_CHARS must be a positive integer: {raw!r}")
    return int(raw)


MEMORY_LIMIT = _resolve_memory_limit()
TEMP_DIR = _resolve_temp_dir()
TOOL_CONCURRENCY = _resolve_tool_concurrency()
REPLY_CHARS = _resolve_reply_chars()
#: list_sources' page name for the tables the dictionary gives no subject; their group's subject stays null.
UNASSIGNED_SUBJECT = "unassigned"
#: The default previews unavailable declared tables; the views page names them all.
#: Keep the preview bounded so unavailable names cannot crowd out the loaded table names.
UNAVAILABLE_TABLES_LISTED = 8
#: Owner choice 2026-11-05 (ss-kj95/ss-qm55): omit these intentional-empty
#: tables and the five audited small retained families from the default page.
#: This is presentation only. Keep registration, explicit pages and queries.
#: Generic coverage kind ``sampled`` includes many other useful populations.
DEFAULT_DISCOVERY_EXCLUDED_TABLES = frozenset({
    "financial_changes", "section_classifications", "bill_summaries", "diff_summaries",
    "edis_documents", "edis_attachments", "edis_attachment_files",
    "sec_rulemakings", "sec_comment_files",
    "cftc_rule_observations", "cftc_comments", "cftc_comment_files",
    "inspectors_general_report_metadata", "supreme_court_index_observations",
    "supreme_court_opinion_observations", "supreme_court_pdf_observations",
})
#: Room a refusal keeps for the clause it asks the caller to add (ORDER BY, LIMIT, OFFSET), which the reply echoes.
RE_ASK_CHARS = 100


def _resolve_catalog_config() -> dict[str, str] | None:
    """R2 catalog config from the environment, or None when a credential is missing.

    Values are interpolated into SQL, so illegal characters raise RuntimeError.
    """
    uri = os.environ.get("R2_CATALOG_URI")
    warehouse = os.environ.get("R2_CATALOG_WAREHOUSE")
    token = os.environ.get("R2_CATALOG_TOKEN")
    if not (uri and warehouse and token):
        return None
    config = {
        "uri": uri,
        "warehouse": warehouse,
        "token": token,
        "namespace": os.environ.get("R2_CATALOG_NAMESPACE") or DEFAULT_CATALOG_NAMESPACE,
    }
    for key, value in config.items():
        if any(c in value for c in ("'", "\\", "\x00", "\n", "\r")):
            raise RuntimeError(f"R2 catalog {key} contains illegal characters")
    return config


def _jsonify(value: Any) -> Any:
    """Convert a DuckDB result value (dates, Decimal, UUID, bytes, containers) to a JSON-safe value."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return str(value)


READ_ONLY_STATEMENT_TYPES = frozenset({"SELECT"})


def _first_write_statement(cursor: duckdb.DuckDBPyConnection, sql: str) -> str | None:
    """Name of the first non-read-only statement in ``sql``, else None.

    Native file permissions constrain reads to selected dataset files; this
    additional gate prevents writes to those files and changes to the database
    or configuration. LocalFileSystem remains enabled for httpfs and spill.

    Classification comes from DuckDB's own parser rather than a prefix regex,
    so leading comments, string literals, and stacked statements cannot smuggle
    a write past it. ``DESCRIBE``/``SHOW``/``SUMMARIZE``/``VALUES``/``TABLE``
    and the FROM-first shorthand all parse as SELECT. EXPLAIN is refused because
    EXPLAIN ANALYZE executes its inner statement, including writes, and the
    exposed parser result does not identify that inner statement's type.

    Matching on ``StatementType.name`` rather than the enum member keeps this
    working against duckdb's incomplete type stubs, which do not declare the
    members, without a blanket type-ignore over the comparison.

    A ``ParserException`` propagates untouched: malformed SQL should surface
    DuckDB's own message, which names the offending token.
    """
    for statement in cursor.extract_statements(sql):
        name = statement.type.name
        if name not in READ_ONLY_STATEMENT_TYPES:
            return name
    return None


def _tables_named(cursor: duckdb.DuckDBPyConnection, sql: str) -> set[str]:
    """Lower-cased names of the tables ``sql`` reads, from DuckDB's parse tree; views are not expanded.

    ``cursor.get_table_names`` binds the query and expands each view to the
    tables under it, which for these ``read_parquet`` views is none, so the
    unbound tree from ``json_serialize_sql`` is walked instead. An unqualified
    name that a CTE in scope defines reads the CTE: a CTE body sees the CTEs
    before it and, when recursive, itself. Other schemas are not tables of this
    database; callers intersect the result with the published tables.
    """
    # extract_statements normalizes read-only PRAGMA forms to SELECT. Serializing
    # the original text instead returns an error object, hiding all its reads.
    stack: list[tuple[Any, frozenset[str]]] = []
    for statement in cursor.extract_statements(sql):
        [(serialized,)] = cursor.execute("SELECT json_serialize_sql(?)", [statement.query]).fetchall()
        tree = json.loads(serialized)
        if tree.get("error"):
            raise ValueError("Cannot inspect SQL relations: " + tree.get("error_message", "unsupported SQL"))
        stack.append((tree, frozenset()))
    names: set[str] = set()
    [(catalog,)] = cursor.execute("SELECT current_database()").fetchall()
    catalog = catalog.lower()
    while stack:
        node, ctes = stack.pop()
        if isinstance(node, list):
            stack.extend((item, ctes) for item in node)
            continue
        if not isinstance(node, dict):
            continue
        if node.get("type") == "TABLE_FUNCTION" and node.get("function", {}).get("function_name", "").lower() in {"query", "query_table", "json_execute_serialized_sql", "pragma_storage_info"}:
            # Storage statistics include source values and resolve their table
            # argument dynamically, outside the named-relation access checks.
            raise ValueError("Dynamic query functions are not supported; name the source relations directly")
        if node.get("type") == "BASE_TABLE":
            name, schema = node["table_name"].lower(), node["schema_name"].lower()
            source_catalog = node["catalog_name"].lower()
            # DuckDB also accepts catalog.table: its unbound tree places that
            # catalog in schema_name, before binding decides which it names.
            if source_catalog in ("", catalog) and (schema == "main" or
                    (not source_catalog and schema == catalog) or (not schema and name not in ctes)):
                names.add(name)
            continue
        if node.get("type") == "RECURSIVE_CTE_NODE":
            ctes |= {node["cte_name"].lower()}
        cte_map = node.get("cte_map")
        for entry in cte_map["map"] if isinstance(cte_map, dict) else ():
            stack.append((entry["value"], ctes))
            ctes |= {entry["key"].lower()}
        stack.extend((value, ctes) for key, value in node.items() if key != "cte_map")
    return names


@_diagnostic_phase('security_settings')
def _apply_security_settings(con: duckdb.DuckDBPyConnection, allowed_paths: list[str] | None = None) -> None:
    """Restrict file reads to the selected dataset members, then lock the session settings."""
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET autoinstall_known_extensions=false")
    con.execute("SET autoload_known_extensions=false")
    con.execute("SET allow_unsigned_extensions=false")
    if con.execute("SELECT current_setting('allow_persistent_secrets')").fetchone() != (False,):
        con.execute("SET allow_persistent_secrets=false")
    con.execute("SET allowed_paths=?", [allowed_paths or []])
    if MEMORY_LIMIT is not None:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{TEMP_DIR}'")
    con.execute("SET enable_external_access=false")
    con.execute("SET lock_configuration=true")


class _Publication(NamedTuple):
    """The published pointers a remote connection pins: the publication index, the rulemaking snapshot and the
    comments export receipt, which states the rows of two fixed-URL files without making them immutable."""

    index: dict
    rulemaking: dict | None
    comments: dict | None = None


class _RemoteSourceView(NamedTuple):
    name: str
    urls: list[str]
    columns: list[list[str]] | None
    required: bool
    export: bool
    #: The view body when it is not ``SELECT *`` over ``urls`` (archive views name columns and add list_row).
    select: str | None = None
    #: Skip on any read failure instead of refusing; the caller retracts the source's whole group.
    degrade: bool = False


def _admit_remote_source_view(cursor, source: _RemoteSourceView) -> bool:
    """Bind and freshly check one independent source; only absent legacy files may skip."""
    from spicy_regs.sources.publication import parquet_scan

    try:
        with _diagnostic_phase("view_registration", source.name):
            cursor.execute(f'CREATE VIEW "{source.name}" AS '
                           + (source.select or f"SELECT * FROM {parquet_scan(source.urls)}"))
        if source.columns is not None or source.required:
            with _diagnostic_phase("view_schema", source.name):
                actual = cursor.execute(f'DESCRIBE "{source.name}"').fetchall()
            if source.columns is not None and [[row[0], row[1]] for row in actual] != source.columns:
                raise RuntimeError(f"Published schema differs from admitted generation: {source.name}")
    except duckdb.Error as exc:
        if source.degrade:
            logger.warning("source %s unreadable at %s; skipping view: %s", source.name, source.urls, exc)
            return False
        if source.required or not (isinstance(exc, duckdb.HTTPException) and exc.status_code == 404):
            kind = "Published generation member" if source.required else "Legacy table"
            raise RuntimeError(f"{kind} unavailable: {source.name}") from exc
        logger.warning("table %s not available at %s; skipping view: %s", source.name, source.urls, exc)
        return False
    return True


def _admit_remote_source_views(con, sources: list[_RemoteSourceView]) -> list[bool]:
    """Use at most four private cursors, settling all work before closing or exposing the database."""
    if not sources:
        return []
    from spicy_regs.runtime_bounds import current_bound

    bound = current_bound()
    workers = min(4, len(sources))
    cursors = []
    stop = threading.Event()
    admitted = [False] * len(sources)

    def batch(cursor, positions):
        try:
            with bound.scope() if bound is not None else nullcontext():
                with bounded_connection(cursor):
                    result = []
                    for position in positions:
                        checkpoint()
                        if stop.is_set():
                            break
                        result.append((position, _admit_remote_source_view(cursor, sources[position])))
                    return result
        except BaseException:
            stop.set()
            raise

    try:
        # Duplicated cursors share global budgets/security, but LOCAL settings
        # reset in DuckDB 1.5.5. Apply the same home and retry policy explicitly.
        for _ in range(workers):
            cursor = con.cursor()
            cursors.append(cursor)
            cursor.execute("SET home_directory=?", [HOME_DIRECTORY])
            for name, value in INTERACTIVE_HTTP_RETRIES.items():
                cursor.execute(f"SET {name}=?", [value])
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="source-admission") as pool:
            futures = [pool.submit(batch, cursor, range(i, len(sources), workers))
                       for i, cursor in enumerate(cursors)]
            for future in futures:
                for position, available in future.result():
                    admitted[position] = available
    finally:
        # Executor shutdown joins running workers even after failure. No child
        # may still use this database when the caller closes or locks it.
        close_error = None
        for cursor in cursors:
            try:
                cursor.close()
            except BaseException as exc:
                close_error = close_error or exc
        if close_error is not None:
            raise close_error
    return admitted


def _immutable_metadata_inputs(index, rulemaking, allowed_paths, archive=None) -> dict[str, str] | None:
    """Classify every admitted SQL URL from validated records, or retain cache defaults.

    ``archive`` is the connection's verified archive binding: its release prefix is create-only, so its
    files are immutable inputs too, and admitting them must not turn the cache off for every query.
    """
    from spicy_regs.sources.publication import (
        PublicationError, SNAPSHOT_POINTER, load_rulemaking_snapshot, parse_index,
        receipt_key_members, receipt_members, rulemaking_receipt, table_members,
    )

    base = R2_BASE_URL.rstrip("/")
    if DATA_DIR is not None or urlsplit(base).scheme not in {"http", "https"} or not allowed_paths:
        return None
    try:
        captured = parse_index(json.dumps(index).encode())
        owned = {}
        for entry in captured["families"].values():
            for key in entry["tables"]:
                owned.update({f"{base}/{member.path}": "managed_generation"
                              for member in table_members(captured, key)})
        owned.update({f"{base}/{member.path}": "managed_generation"
                      for member in (*receipt_members(captured), *receipt_key_members(captured))})
        if rulemaking is not None:
            # Reuse the maintained pointer/manifest validator with the captured
            # documents only: no additional HTTP read or filename inference.
            manifest = rulemaking["manifest"]
            pointer = {"format_version": manifest["format_version"], "dataset": manifest["dataset"],
                       "snapshot_id": rulemaking["snapshot_id"], "manifest_key": rulemaking["manifest_key"]}
            documents = {f"{base}/{SNAPSHOT_POINTER}": json.dumps(pointer).encode(),
                         f"{base}/{rulemaking['manifest_key']}": json.dumps(manifest).encode()}
            validated = load_rulemaking_snapshot(base, read=lambda url, **_: documents[url])
            if validated is None or validated != rulemaking:
                return None
            owned.update({f"{base}/{record['remote_key']}": "rulemaking_snapshot"
                          for record in validated["tables"].values()})
            # The build admits the snapshot's declared receipt member too; it sits under the same snapshot prefix.
            if (receipt := rulemaking_receipt(validated)) is not None:
                owned[f"{base}/{receipt['remote_key']}"] = "rulemaking_snapshot"
        if archive is not None and urlsplit(archive.origin).scheme == "https":
            owned.update({archive.url(stem): "archive_release" for stem in archive.files})
        if any(path not in owned for path in allowed_paths):
            return None
        return {path: owned[path] for path in allowed_paths}
    except (PublicationError, ValueError, TypeError, KeyError, AttributeError):
        return None


def _enable_immutable_metadata_cache(con, index, rulemaking, allowed_paths, archive=None):
    """Reuse HTTP headers and parsed Parquet metadata after fresh immutable-input admission.

    Rulemaking immutability is the maintained source-owner policy, weaker than
    managed generation create-only writes. Neither premise covers legacy mirrors.
    """
    inputs = _immutable_metadata_inputs(index, rulemaking, allowed_paths, archive)
    if inputs is not None:
        con.execute("SET enable_http_metadata_cache=true")
        con.execute("SET GLOBAL parquet_metadata_cache=true")
    return inputs


@_diagnostic_phase('publication')
def _read_publication() -> _Publication:
    """The live pointers, read once each; the rulemaking snapshot and comments receipt are ``None`` while unpublished.

    An invalid comments receipt reads as none: it only labels two legacy files' rows, and refusing the
    connection over it would take every table down. A failed read propagates, as the other pointers' do.
    """
    from spicy_regs.sources.publication import (
        PublicationError,
        load_comments_publication,
        load_index,
        load_rulemaking_snapshot,
    )

    try:
        with _diagnostic_phase("comments_pointer"):
            comments = load_comments_publication(R2_BASE_URL)
    except PublicationError:
        logger.warning("comments export receipt is invalid; its tables state no rows", exc_info=True)
        comments = None
    with _diagnostic_phase("publication_index"):
        index = load_index(R2_BASE_URL)
    with _diagnostic_phase("rulemaking_pointer"):
        rulemaking = load_rulemaking_snapshot(R2_BASE_URL)
    return _Publication(index, rulemaking, comments)


def _pinned_publication(con: duckdb.DuckDBPyConnection) -> _Publication:
    """The pointers ``con`` pinned when it was built, as :func:`_read_publication` returned them."""
    cursor = con.cursor()
    export = _pinned_record(cursor, "_spicy_comments_export")
    return _Publication(
        _connection_index(cursor), _pinned_record(cursor, "_spicy_rulemaking"), export["receipt"] if export else None
    )


def _comments_exports(comments: dict, served: list[str]) -> dict[str, dict]:
    """The receipt's pin for each served export table, with whether its object still matches the receipt.

    One HEAD per served file (``COMMENTS_EXPORT_TABLES``), not per file the receipt lists: only these back views.
    """
    from spicy_regs.sources.publication import comments_export_pins, mutable_versions_match

    pins = comments_export_pins(R2_BASE_URL, comments, served)
    return {name: {**pin, "matches_object": mutable_versions_match({name: pin})} for name, pin in pins.items()}


_discovery_only = ContextVar("spicy_schema_discovery", default=False)
_physical_receipt_lookup = ContextVar("spicy_physical_receipt_lookup", default=False)
_requested_relations: ContextVar[set[str] | None] = ContextVar("spicy_requested_relations", default=None)
_DISCOVERY_CATALOG = "_spicy_discovery_catalog"


def _remote_query_scope(con, available_names):
    """Resolve requested remote SQL inputs from declarations before opening source files."""
    requested = _requested_relations.get()
    # No named relations can mean SHOW/catalog introspection. Preserve its
    # complete catalog rather than infer dependencies from arbitrary SQL.
    if DATA_DIR is not None or _discovery_only.get() or not requested:
        return None
    from spicy_regs.relationship_views import describe_relationship_views

    relationships = describe_relationship_views(con, available_names)
    dependencies = {
        name: entry["metadata"].get("declared_dependencies", entry["dependencies"])
        for name, entry in relationships.items()
    }
    dependencies.update({spec.view.name: spec.view.required for spec in _selected_fec_views(con)})
    selected = set(requested)
    while True:
        added = {parent for name in selected for parent in dependencies.get(name, ())} - selected
        if not added:
            return selected
        selected.update(added)


def _archive_views(query_scope) -> list[tuple[str, str]]:
    """The archive views a build admits: all of them for an unscoped build, else only those the query names."""
    from spicy_regs.archive_release import VIEWS

    return [(view, stem) for view, stem in VIEWS.items() if query_scope is None or view in query_scope]


@_diagnostic_phase('archive_release')
def _bind_archive(query_scope):
    """The pinned archive release and its connection record; neither when the query names no archive view.

    Remote: the verified manifest and schema are read once per process. Local: the release is read only from
    ``<SPICY_REGS_DATA_DIR>/archive/releases/<release_id>/`` and every bound file is hashed; a missing copy
    is reported unavailable and never fetched. An unscoped remote build (discovery, catalog SQL) reports an
    unreachable archive as unavailable rather than taking every table down (an unreadable release file is
    handled the same way at admission, see ``_RemoteSourceView.degrade``); a query that names an archive
    view, or a corrupt local copy, refuses.
    """
    from spicy_regs import archive_release
    from spicy_regs.sources.publication import PublicationError

    if query_scope is not None and not query_scope & set(archive_release.VIEWS):
        return None, None
    release = archive_release.load_pins()["bound"]
    origin = str(DATA_DIR / "archive") if DATA_DIR is not None else archive_release.ORIGIN
    if DATA_DIR is not None and not Path(origin, "releases", release, "manifest.json").is_file():
        return None, {"status": "unavailable", "release_id": release,
                      "reason": "No local copy under SPICY_REGS_DATA_DIR/archive/releases/<release_id>/; "
                                "a local reader never falls back to the network."}
    try:
        binding = archive_release.bind(release, origin, hash_files=DATA_DIR is not None)
    except (PublicationError, httpx.HTTPError, OSError, ValueError, KeyError, TypeError) as exc:
        if query_scope is not None or DATA_DIR is not None:
            raise RuntimeError(f"Archive release {release[:8]} unavailable: {exc}") from exc
        logger.warning("archive release %s unavailable; its views are not bound: %s", release, exc)
        return None, {"status": "unavailable", "release_id": release, "reason": str(exc)}
    return binding, archive_release.description(binding)


@_diagnostic_phase('connection_build')
def _build_connection(publication: _Publication | None = None) -> duckdb.DuckDBPyConnection:
    """Discard cancelled/failed admissions; never cache a partially built connection."""
    checkpoint()
    if DATA_DIR is None and _resolve_catalog_config() is not None:
        raise RuntimeError(
            "MCP catalog reads require dynamic file access, which this restricted SQL server refuses; "
            "serve published Parquet or SPICY_REGS_DATA_DIR instead"
        )
    con = duckdb.connect()
    try:
        with bounded_connection(con):
            return _build_connection_inputs(con, publication)
    except BaseException:
        con.close()
        raise


def _build_connection_inputs(con, publication):
    """Open a DuckDB connection with one view per available table, pinned to one publication snapshot.

    A remote connection pins ``publication``, read here when not given; a
    rulemaking table's view reads the snapshot the pointer names. Local managed
    bytes are rehashed before their views are created. A view whose schema
    differs from its admitted generation, a managed or snapshot member that
    cannot be read, or a legacy table that fails other than as absent (HTTP
    404), raises RuntimeError.
    """
    from spicy_regs.sources.publication import COMMENTS_EXPORT_TABLES, parquet_scan, table_descriptor, table_members

    if DATA_DIR is None and os.environ.get("SPICY_REGS_DOCSPEC_EXPORTS"):
        raise RuntimeError("DocSpec exports require an explicit SPICY_REGS_DATA_DIR")
    local = None
    signatures = {}
    comments = None
    if DATA_DIR is not None:
        from spicy_regs.local_data import local_selection, verify_local_members

        local = local_selection(DATA_DIR, admit_rows=not _discovery_only.get())
        signatures = verify_local_members(local)
        publication_index, rulemaking = local.publication, None
    else:
        publication_index, rulemaking, comments = publication or _read_publication()
    catalog_only = DATA_DIR is None and _discovery_only.get() and not _physical_receipt_lookup.get()
    checkpoint()
    con.execute("SET allow_persistent_secrets=false")
    con.execute(f"SET home_directory='{HOME_DIRECTORY.replace(chr(39), chr(39) * 2)}'")
    if DATA_DIR is None and not catalog_only:
        with _diagnostic_phase("httpfs_load"):
            load_public_http(con, INTERACTIVE_HTTP_RETRIES)

    allowed_paths: list[str] = []
    # Request cursors share regular in-memory tables, not connection-local
    # temporary tables. Keep the pin alongside the views they actually query.
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(publication_index)])
    if catalog_only:
        receipt_columns = _selected_receipt_columns(publication_index)
        con.execute(f"CREATE TABLE {_DISCOVERY_CATALOG} (snapshot VARCHAR)")
        con.execute(f"INSERT INTO {_DISCOVERY_CATALOG} VALUES (?)", [json.dumps({"receipt_columns": receipt_columns})])
    if rulemaking is not None:
        con.execute("CREATE TABLE _spicy_rulemaking (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_rulemaking VALUES (?)", [json.dumps(rulemaking)])
    if local is not None and (local.is_download or local.native or local.docspec):
        con.execute("CREATE TABLE _spicy_local_selection (snapshot VARCHAR)")
        con.execute(
            "INSERT INTO _spicy_local_selection VALUES (?)",
            [
                json.dumps(
                    {
                        "directory": str(local.directory),
                        "signatures": signatures,
                        "docspec": local.docspec,
                        "docspec_configuration": local.docspec_configuration,
                        "selected_tables": list(local.files),
                        "receipt_rows_admitted": not _discovery_only.get(),
                        "receipt_members": {name: str(path) for name, path in local.receipts.items()},
                        "receipt_indexes": {name: str(path) for name, path in local.receipt_indexes.items()},
                        "native": {name: {"subjects": [str(p) for p in value.subjects],
                                           "receipts": str(value.receipts), "generation_id": value.generation_id,
                                           "key_index": str(value.key_index) if value.key_index else None,
                                           "key_index_descriptor": value.key_index_descriptor}
                                   for name, value in local.native.items()},
                    }
                )
            ],
        )
    # These newly declared subjects enter serving only through a managed
    # generation or explicit local/rulemaking selection, never a bare legacy key.
    managed_only = {
        "cms_part_d_drug_spending",
        "cms_medicaid_drug_spending",
        "cms_part_b_drug_spending",
        "treasury_debt_observations", "treasury_statement_periods", "treasury_statement_classification_cells",
        "grants_opportunity_observations",
        "ofac_entity_observations",
        "ofac_snapshots",
        "executive_order_observations",
        "fec_financial_profiles",
        "census_acs_county_observations",
        "administration_policy_bill_links", "administration_policy_statements",
        "ecfr_snapshot_sections", "ecfr_snapshots", "federal_register_issues",
        "table3_historical_records", "uscode_edition_structure", "uscode_editions", "uscode_edition_sections",
    }
    managed_names = [
        key.removesuffix(".parquet") for e in publication_index["families"].values() for key in e["tables"]
    ]
    snapshot_names = [key.removesuffix(".parquet") for key in (rulemaking or {"tables": {}})["tables"]]
    selected_names = list(local.files) if local is not None else []
    source_names = list(dict.fromkeys((*TABLES, *managed_names, *snapshot_names, *selected_names)))
    query_scope = _remote_query_scope(con, source_names)
    if query_scope is not None:
        con.execute("CREATE TABLE _spicy_query_scope (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_query_scope VALUES (?)", [json.dumps({"relations": sorted(query_scope)})])
    exports: list[str] = []
    remote_sources: list[_RemoteSourceView] = []
    archive, archive_record = _bind_archive(query_scope)
    for view, stem in (_archive_views(query_scope) if archive is not None else ()):
        if local is None:
            # Unscoped (discovery) builds tolerate an unreadable release file, e.g. a release pruned under a
            # manifest cached for the process; a query that names the view still refuses.
            remote_sources.append(_RemoteSourceView(view, [archive.url(stem)], None, True, False,
                                                    archive.view_sql(stem), degrade=query_scope is None))
            continue
        try:
            with _diagnostic_phase("view_registration", view):
                con.execute(f'CREATE VIEW "{view}" AS {archive.view_sql(stem)}')
        except duckdb.Error as exc:
            con.close()
            raise RuntimeError(f"Archive release file unavailable: {view}") from exc
        allowed_paths.append(archive.url(stem))
    for name in source_names:
        if query_scope is not None and name not in query_scope:
            continue
        checkpoint()
        published = table_descriptor(publication_index, f"{name}.parquet")
        if catalog_only and published is not None:
            continue
        paths = [member.path for member in table_members(publication_index, f"{name}.parquet")]
        pinned = rulemaking["tables"].get(f"{name}.parquet") if rulemaking is not None and published is None else None
        if local is None and name in managed_only and published is None and pinned is None:
            continue
        if pinned is not None:
            paths = [pinned["remote_key"]]
        if local is not None:
            if name not in local.files:
                continue
            urls = [str(path) for path in local.paths(name)]
        else:
            urls = [f"{R2_BASE_URL}/{path}" for path in paths]
        if local is None:
            remote_sources.append(_RemoteSourceView(
                name, urls, published["columns"] if published is not None else None,
                published is not None or pinned is not None,
                published is None and pinned is None and name in COMMENTS_EXPORT_TABLES,
            ))
            continue
        try:
            if not urls and local is not None and name in local.native:
                import pyarrow as pa
                from spicy_regs.etl_policy_registry import installed_policies
                con.register("_empty_native", pa.Table.from_batches([], schema=installed_policies()[name].subject_schema))
                con.execute(f'CREATE TABLE "{name}" AS SELECT * FROM _empty_native')
                con.unregister("_empty_native")
            else:
                with _diagnostic_phase("view_registration", name):
                    con.execute(f'CREATE VIEW "{name}" AS SELECT * FROM {parquet_scan(urls)}')
            if published is not None:
                with _diagnostic_phase("view_schema", name):
                    actual = con.execute(f'DESCRIBE "{name}"').fetchall()
                if [[row[0], row[1]] for row in actual] != published["columns"]:
                    con.close()
                    raise RuntimeError(f"Published schema differs from admitted generation: {name}")
            allowed_paths.extend(urls)
            if local is None and published is None and pinned is None and name in COMMENTS_EXPORT_TABLES:
                exports.append(name)
        except duckdb.Error as exc:
            required = published is not None or pinned is not None or (local is not None and (local.is_download or local.native or local.docspec))
            # A remote legacy table is skipped only when it is absent; a throttled or failing read refuses the
            # build, so a refresh keeps the connection it would replace instead of serving one without the table.
            absent = local is not None or (isinstance(exc, duckdb.HTTPException) and exc.status_code == 404)
            if required or not absent:
                con.close()
                kind = "Published generation member" if required else "Legacy table"
                raise RuntimeError(f"{kind} unavailable: {name}") from exc
            logger.warning("table %s not available at %s; skipping view: %s", name, urls, exc)
    if catalog_only and remote_sources:
        with _diagnostic_phase("httpfs_load"):
            load_public_http(con, INTERACTIVE_HTTP_RETRIES)
    try:
        admitted = _admit_remote_source_views(con, remote_sources)
    except BaseException:
        con.close()
        raise
    unreadable = [source.name for source, available in zip(remote_sources, admitted) if source.degrade and not available]
    if unreadable:
        # The archive is bound whole or not at all: retract its admitted views and report it unavailable.
        for source, available in zip(remote_sources, admitted):
            if source.degrade and available:
                con.execute(f'DROP VIEW "{source.name}"')
        admitted = [available and not source.degrade for source, available in zip(remote_sources, admitted)]
        archive, archive_record = None, {"status": "unavailable", "release_id": archive_record["release_id"],
                                         "reason": f"Unreadable release files: {', '.join(unreadable)}"}
    for source, available in zip(remote_sources, admitted):
        if available:
            allowed_paths.extend(source.urls)
            if source.export:
                exports.append(source.name)
    if archive_record is not None:
        con.execute("CREATE TABLE _spicy_archive (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_archive VALUES (?)", [json.dumps(archive_record)])
    if comments is not None:
        try:
            matched = _comments_exports(comments, exports)
        except Exception:
            con.close()
            raise
        con.execute("CREATE TABLE _spicy_comments_export (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_comments_export VALUES (?)", [json.dumps({"receipt": comments, "tables": matched})])
    # Shared ETL receipts are processing evidence, selected per family from the
    # same captured index as the subjects above, never fabricated subject tables.
    # A missing member refuses the connection rather than falling back to another
    # generation's evidence.
    from spicy_regs.sources.publication import receipt_members

    # A requested shared log still means every captured family. Otherwise,
    # selected receipt-dependent adapters admit their own exact members; a
    # source-only query need not open unrelated receipt files or key indexes.
    receipts = (receipt_members(publication_index)
                if not catalog_only and (query_scope is None or "etl_receipts" in query_scope) else ())
    local_receipts = local.receipts if local is not None else None
    urls = (list(map(str, local_receipts.values())) if local_receipts is not None
            else [f"{R2_BASE_URL}/{member.path}" for member in receipts])
    if urls:
        try:
            if local is not None and local.native:
                groups = {}
                for name, value in local.native.items():
                    groups.setdefault(str(value.receipts), []).append(name)
                scans = [f"SELECT * FROM {parquet_scan([path])} WHERE dataset IN (" +
                         ",".join("'" + name.replace("'", "''") + "'" for name in names) + ")"
                         for path, names in groups.items()]
                con.execute('CREATE VIEW etl_receipts AS ' + ' UNION ALL '.join(scans))
            else:
                with _diagnostic_phase('receipt_registration'):
                    con.execute(f'CREATE VIEW etl_receipts AS SELECT * FROM {parquet_scan(urls)}')
            with _diagnostic_phase('receipt_schema'):
                actual = [[row[0], row[1]] for row in con.execute('DESCRIBE etl_receipts').fetchall()]
            # Only the members this view reads: an unselected family's receipts do not gate a local download.
            selected = set(local_receipts) if local_receipts is not None else {member.path for member in receipts}
            if any(entry["etlReceipts"]["columns"] != actual for entry in publication_index["families"].values()
                   if "etlReceipts" in entry and f"{entry['prefix']}/{entry['etlReceipts']['key']}" in selected):
                raise RuntimeError("Published ETL receipt schema differs from the admitted generation")
            allowed_paths.extend(urls)
        except (duckdb.Error, RuntimeError) as exc:
            con.close()
            raise RuntimeError("Published ETL receipt member unavailable") from exc
    from spicy_regs.receipt_key_index import check_reader

    selected_receipt_paths = set(local_receipts) if local_receipts is not None else {m.path for m in receipts}
    for entry in publication_index["families"].values():
        specification = entry.get("etlReceipts", {})
        key = specification.get("keyIndex")
        receipt_path = f"{entry['prefix']}/{specification.get('key', '')}"
        if key is None or receipt_path not in selected_receipt_paths:
            continue
        member_path = f"{entry['prefix']}/{key['key']}"
        location = str(local.receipt_indexes[member_path]) if local is not None else f"{R2_BASE_URL}/{member_path}"
        try:
            with _diagnostic_phase("receipt_key_index"):
                check_reader(con, location, key, specification)
        except ValueError:
            con.close()
            raise
        allowed_paths.append(location)
    if local is not None:
        for value in local.native.values():
            if value.key_index is not None:
                import pyarrow.parquet as pq
                from spicy_regs.selected_generations import _pin
                check_reader(con, str(value.key_index), value.key_index_descriptor,
                             {"sha256": _pin(value.receipts), "byteSize": value.receipts.stat().st_size,
                              "rows": pq.ParquetFile(value.receipts).metadata.num_rows})
                allowed_paths.append(str(value.key_index))
    if local is None and rulemaking is not None and not catalog_only:
        from spicy_regs.sources.publication import rulemaking_receipt

        snapshot_receipt = rulemaking_receipt(rulemaking)
        if snapshot_receipt is not None:
            # The lookup reads this internal member directly. Admit only the receipt paired with
            # the connection's captured subjects before external access is locked.
            allowed_paths.append(f"{R2_BASE_URL.rstrip('/')}/{snapshot_receipt['remote_key']}")
    try:
        immutable_inputs = _enable_immutable_metadata_cache(con, publication_index, rulemaking, allowed_paths, archive)
        _install_relationship_views(con)
        if immutable_inputs is not None and immutable_inputs != _immutable_metadata_inputs(
                publication_index, rulemaking, allowed_paths, archive):
            raise RuntimeError("Immutable SQL inputs changed before the security lock")
        from spicy_regs.relationship_views.reference import reference_paths

        # The vendored reference files are package bytes, not immutable published members: admit them after the
        # metadata-cache check so they do not turn that cache off.
        _apply_security_settings(con, [*allowed_paths, *reference_paths()])
    except BaseException:
        con.close()
        raise
    return con


@lru_cache(maxsize=16)
def _parsed_pin(raw: str) -> dict:
    """A pinned JSON record, parsed once per distinct document, so once per connection rather than per helper call.

    One tool call reads the same pins in several helpers; the index alone is tens of kilobytes. Callers must not
    mutate the shared result.
    """
    return json.loads(raw)


def _connection_index(cursor: duckdb.DuckDBPyConnection) -> dict:
    """The publication snapshot pinned in this connection; injected local connections report an empty family map."""
    try:
        row = cursor.execute("SELECT snapshot FROM _spicy_publication").fetchone()
        if row is None:
            raise RuntimeError("Missing publication snapshot")
        return _parsed_pin(row[0])
    except duckdb.CatalogException:
        # Explicitly injected local connections have no publication admission.
        return {"families": {}}


def _pinned_record(cursor: duckdb.DuckDBPyConnection, table: str) -> dict | None:
    """The JSON record this connection pinned in ``table`` when it was built, or None when it pinned none."""
    try:
        row = cursor.execute(f"SELECT snapshot FROM {table}").fetchone()
        return _parsed_pin(row[0]) if row is not None else None
    except duckdb.CatalogException:
        return None


def _connection_generation(cursor: duckdb.DuckDBPyConnection) -> str:
    """A short digest of every pin this connection holds, so a client paging list_sources can tell its pages match."""
    digest = hashlib.sha256()
    for table in ("_spicy_publication", "_spicy_rulemaking", "_spicy_local_selection", "_spicy_archive"):
        try:
            row = cursor.execute(f"SELECT snapshot FROM {table}").fetchone()
        except duckdb.CatalogException:
            row = None
        digest.update(f"{table}\0{row[0] if row else ''}\0".encode())
    return digest.hexdigest()[:16]


def _connection_local_selection(cursor: duckdb.DuckDBPyConnection) -> dict | None:
    """The local-selection snapshot pinned in this connection, or None when it has none."""
    return _pinned_record(cursor, "_spicy_local_selection")


def _connection_rulemaking(cursor: duckdb.DuckDBPyConnection) -> dict:
    """The rulemaking snapshot pinned in this connection; empty when it read none (local, injected or unpublished)."""
    return _pinned_record(cursor, "_spicy_rulemaking") or {"snapshot_id": None, "tables": {}}


def _connection_relationships(cursor: duckdb.DuckDBPyConnection) -> dict:
    """Definitions and dependency availability bound alongside this connection's source views."""
    return _pinned_record(cursor, "_spicy_relationships") or {}


def _selected_fec_views(con):
    local = _connection_local_selection(con)
    return selected_qualified_views(
        FEC_QUALIFIED_VIEWS, FEC_LEGACY_QUALIFIED_VIEWS, _connection_index(con),
        local.get("native", {}) if local is not None else None,
    )


def _fec_release_configuration(con, publication):
    """Deployment chooses the receipt after image build; runtime identities are measured here."""
    if not FEC_QUALIFIED_VIEWS:
        return None
    from spicy_regs.fec_release import capture_configuration

    path = os.environ.get("SPICY_REGS_FEC_RELEASE_FILE")
    return capture_configuration(
        _selected_fec_views(con),
        receipt_digest=os.environ.get("SPICY_REGS_FEC_RELEASE_SHA256"),
        image_digest=os.environ.get("SPICY_REGS_CONSUMER_IMAGE_DIGEST"),
        base_url=R2_BASE_URL, local_path=Path(path) if path else None,
        local_mode=DATA_DIR is not None, publication=publication,
    )


def _fec_release_reply(cursor):
    """Summarize captured release states; selected descriptions retain full evidence."""
    selected = _pinned_record(cursor, "_spicy_fec_release")
    return {
        "receipt_sha256": selected.get("receipt_sha256") if selected else None,
        "consumer": selected.get("consumer") if selected else None,
        "status_counts": dict(Counter(
            info["release_compatibility"]["status"]
            for info in _connection_relationships(cursor).values() if "release_compatibility" in info
        )),
        "raw_query_financial_qualification": "not_inferred",
        "details": "Call describe_table for a selected view's release compatibility evidence and reasons.",
    }


@_diagnostic_phase('relationship_binding')
def _install_relationship_views(con: duckdb.DuckDBPyConnection) -> None:
    """Bind trusted definitions and verify selected FEC receipt inputs before locking."""
    from spicy_regs.relationship_views import describe_relationship_views, install_relationship_views

    status = _publication_status(con)
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter, receipt_owner
    from spicy_regs.relationship_views.fec import FEC_VIEWS
    from spicy_regs.relationship_views.sql_views import install_sql_views
    index = _connection_index(con)
    from spicy_regs.subject_catalog import descriptors
    declared = descriptors()
    local_selection = _connection_local_selection(con)
    processing = {name for family in index["families"].values()
                  if local_selection is None or
                  f"{family['prefix'].rstrip('/')}/{family.get('etlReceipts', {}).get('key')}"
                  in local_selection.get("receipt_members", {})
                  for name in family.get("etlReceipts", {}).get("datasets", ())
                  if declared.get(name, {}).get("receipt_only")}
    native = local_selection.get("native", {}) if local_selection else {}
    processing |= {name for name in native if declared.get(name, {}).get("receipt_only")}
    local_directory = local_selection["directory"] if local_selection else DATA_DIR
    adapter = ReceiptAdapter(con, index, R2_BASE_URL, local_directory=local_directory,
                             local_receipts=({**local_selection.get("receipt_members", {}),
                                              **local_selection.get("receipt_indexes", {})} if local_selection else None),
                             local_native=native)
    # A mixed old/native dependency set is a refusal, never a pass-through.
    adapted = [spec for spec in FEC_VIEWS
               if set(spec.required) <= set(status["tables"]) | processing
               and any(receipt_owner(index, table) or table in native for table in spec.required)]
    for spec in adapted:
        adapter.require_selected(spec.required)
    requested = _requested_relations.get()
    selected = None if requested is None else set(requested)
    if _discovery_only.get() or requested is not None:
        relationships = describe_relationship_views(con, status["tables"], publication=status["publication"])
    else:
        relationships = {}
    if not _discovery_only.get():
        if selected is not None:
            while True:
                parents = {parent for name in selected if name in relationships
                           for parent in relationships[name]["metadata"].get(
                               "declared_dependencies", relationships[name]["dependencies"])}
                added = parents - selected
                if not added:
                    break
                selected.update(added)
        relationships.update(install_relationship_views(
            con, status["tables"] if selected is None else set(status["tables"]) & selected,
            publication=status["publication"], selected_names=selected,
        ))
    for spec in adapted:
        if _discovery_only.get() or selected is not None and spec.name not in selected:
            relationships[spec.name] = {
                "status": "deferred", "reason": "Exact receipt row admission is deferred until a query connection is requested.",
                "dependencies": list(spec.required), "metadata": {
                    "label": spec.name.replace("_", " "), "summary": spec.meaning, "kind": "derived",
                    "rule_version": spec.rule_version, "identity_columns": list(spec.identity_columns),
                    "input_publications": {table: status["publication"].get(table) for table in spec.required},
                    "query_admission": "pending_receipt_rows"}}
            continue
        prepared = adapter.prepare(spec, spec.query(status["publication"]))
        item = install_sql_views(con, prepared.required, [prepared], status["publication"])[spec.name]
        item["dependencies"] = list(spec.required)
        item["metadata"]["input_publications"] = {table: status["publication"].get(table) for table in spec.required}
        item["metadata"]["availability_basis"] = "Pinned subject and receipt bytes and exact row-version joins validated before binding."
        relationships[spec.name] = item
    selected = _fec_release_configuration(con, status["publication"])
    if selected is not None:
        from spicy_regs.fec_release import install_views

        if any(spec.view.name in {*status["tables"], *relationships} for spec in FEC_QUALIFIED_VIEWS):
            raise ValueError("Qualified FEC view collides with an existing table or relationship view")
        relationships.update(install_views(
            con, _selected_fec_views(con), selected, index, set(status["tables"]) | processing, status["publication"],
            read_tables=_tables_named, prepare=adapter.prepare_qualified, defer_admission=_discovery_only.get(),
            admit_names=requested,
        ))
        con.execute("CREATE TABLE _spicy_fec_release (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_fec_release VALUES (?)", [json.dumps(selected)])
    con.execute("CREATE TABLE _spicy_relationships (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_relationships VALUES (?)", [json.dumps(relationships)])


def _publication_status(cursor: duckdb.DuckDBPyConnection) -> dict:
    """Describe selected or admitted relations and pins from this connection's snapshot."""
    index = _connection_index(cursor)
    rulemaking = _connection_rulemaking(cursor)
    available = _available_tables(cursor)
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    managed = {
        key.removesuffix(".parquet"): {
            "status": "managed_download" if local is not None else "managed_generation",
            "family": family,
            "artifact_digest": entry["artifactDigest"],
            **(
                {
                    "verification": "Local bytes rehashed at connection creation; file changes checked around tool statements."
                }
                if local is not None
                else {}
            ),
        }
        for family, entry in index["families"].items()
        for key in entry["tables"]
    }
    if local is not None:
        managed.update({name: {"status": "native_selected", "generation_id": value["generation_id"],
                               "verification": ("Selected bytes rehashed; exact receipt row joins remain pending."
                                                if not local.get("receipt_rows_admitted", True) else
                                                "Selected bytes and exact receipt joins verified; file changes checked around statements.")}
                        for name, value in local.get("native", {}).items()})
    if local is not None:
        managed.update(local.get("docspec", {}))
    receipt_families = _receipt_families(index, local)
    if "etl_receipts" in available and receipt_families:
        managed["etl_receipts"] = {"status": "managed_receipts", "families": receipt_families}
    snapshot = {
        key.removesuffix(".parquet"): {
            "status": "rulemaking_snapshot",
            "snapshot_id": rulemaking["snapshot_id"],
            "sha256": f"sha256:{record['sha256']}",
        }
        for key, record in rulemaking["tables"].items()
    }
    fallback = "local_unversioned" if DATA_DIR is not None else "legacy_unversioned"
    derived = {
        name: {
            "status": "derived_view",
            "rule_version": value["metadata"]["rule_version"],
            "dependencies": value["dependencies"],
            "input_publications": value["metadata"]["input_publications"],
            **({"release_compatibility": value["release_compatibility"]} if "release_compatibility" in value else {}),
        }
        for name, value in _connection_relationships(cursor).items() if value["status"] == "available"
    }
    archive = _pinned_record(cursor, "_spicy_archive") or {}
    archived = {name: {"status": "archive_release", "release_id": archive["release_id"],
                       **{key: view[key] for key in ("file", "sha256", "rows")}}
                for name, view in archive.get("views", {}).items()}
    return {
        "tables": available,
        "declared_tables": list(TABLES),
        "publication": {
            name: derived.get(name) or managed.get(name) or archived.get(name) or snapshot.get(name, {"status": fallback})
            for name in available
        },
        "verification": (
            "Managed local bytes rehashed at connection creation; file changes checked around tool statements."
            if local is not None
            else "Managed remote bytes verified before publication; this reader pins URLs and checks schemas."
            + (
                " Rulemaking snapshot URLs are pinned by the pointer read at connection creation; their manifest"
                " states no columns, so describe_table compares their schemas with the dictionary."
                if snapshot
                else ""
            )
        ),
    }


def _receipt_families(index: dict, local: dict | None) -> dict[str, dict]:
    """Each family whose receipt member this connection's etl_receipts view reads, with its generation pins.

    A local download reads only its selected families' members; a native build root
    pins no publication index, so it has none.
    """
    selected = None if local is None else local.get("receipt_members", {})
    return {
        family: {
            "artifact_digest": entry["artifactDigest"],
            "generation_id": receipt["generationId"],
            "sha256": receipt["sha256"],
            "rows": receipt["rows"],
            "datasets": receipt["datasets"],
        }
        for family, entry in index["families"].items()
        if (receipt := entry.get("etlReceipts")) and (selected is None or f"{entry['prefix']}/{receipt['key']}" in selected)
    }


def _selected_receipt_columns(index: dict) -> list[list[str]] | None:
    """The selected shared log declares one schema across all receipt members."""
    schemas = [entry["etlReceipts"]["columns"] for entry in index["families"].values() if "etlReceipts" in entry]
    if not schemas:
        return None
    if any(schema != schemas[0] for schema in schemas):
        raise RuntimeError("Selected ETL receipt members declare different schemas")
    return schemas[0]


def _pinned_rows(index: dict, rulemaking: dict, name: str) -> int | None:
    """The row count the pinned index or snapshot manifest states for ``name``; None for a table neither pins."""
    from spicy_regs.sources.publication import table_descriptor

    descriptor = table_descriptor(index, f"{name}.parquet")
    if descriptor is not None:
        return descriptor["rows"]
    pinned = rulemaking["tables"].get(f"{name}.parquet")
    return pinned["rows"] if pinned is not None else None


def _published_at(index: dict, name: str) -> dict[str, str | None]:
    """When the publisher moved the pointer to ``name``'s generation (the index's ``publishedAt``) and what that value
    observed: the move (``pointer_move``) or, for a value the 2026-10-03 backfill wrote, the generation's last object
    write, at or before the move (``last_object_write``). Both null for a table no family pins or a generation
    published before the index recorded the instant. Not a data-as-of."""
    from spicy_regs.sources.publication import published_at_basis, table_owner

    owner = table_owner(index, f"{name}.parquet")
    instant = owner[1].get("publishedAt") if owner is not None else None
    return {"published_at": instant, "published_at_basis": published_at_basis(instant)}


#: Each family generation's ``spec.parents`` by (base URL, artifact digest): roots are immutable, so one read
#: serves the process. A concurrent first use may read twice. A failed read is kept only as its time, and the
#: root is not read again for ROOT_RETRY_SECONDS, so a failing bucket costs one GET a minute, not one a reply.
_ROOT_PARENTS: dict[tuple[str, str], dict[str, dict]] = {}
_ROOT_FAILED_AT: dict[tuple[str, str], float] = {}
ROOT_RETRY_SECONDS = 60.0


def _family_parents(base_url: str, entry: Mapping) -> dict[str, dict]:
    """``spec.parents`` of the family generation ``entry`` pins, read on first use; {} when the root records none."""
    from spicy_regs.sources.publication import PublicationError, read_pinned_root

    key = (base_url, entry["artifactDigest"])
    if key in _ROOT_PARENTS:
        return _ROOT_PARENTS[key]
    failed = _ROOT_FAILED_AT.get(key)
    if failed is not None and _monotonic() - failed < ROOT_RETRY_SECONDS:
        raise PublicationError("generation root unavailable at the last read; not retried within a minute")
    for cache in (_ROOT_PARENTS, _ROOT_FAILED_AT):
        if len(cache) >= 1024:  # generations move daily; a long-lived process forgets old ones
            cache.clear()
    try:
        parents = read_pinned_root(base_url, entry).get("spec", {}).get("parents") or {}
    except (PublicationError, httpx.HTTPError, OSError):
        _ROOT_FAILED_AT[key] = _monotonic()
        raise
    _ROOT_FAILED_AT.pop(key, None)
    _ROOT_PARENTS[key] = parents
    return parents


def _all_current(claims: Sequence[bool | None]) -> bool | None:
    """Three-valued: false if any input lags, else null if any is unknown, else true."""
    return False if False in claims else None if None in claims else True


def _export_pin(exports: Mapping[str, dict], table: str, parent: Mapping) -> tuple[str | None, bool | None]:
    """The live pin of the kind ``parent`` recorded (the file's sha256, or its ETag) from the export receipt this
    connection matched at build, and whether the two agree; (None, None) when no receipt pins a matching file."""
    export = exports.get(table)
    if export is None or export["rows_basis"] != "comments_export_receipt":
        return None, None
    kind = "sha256" if "sha256" in parent else "etag"
    live = export["export_receipt"][kind]
    return live, parent[kind] == live


def _input_lineage(index: dict, name: str, exports: Mapping[str, dict]) -> dict[str, Any]:
    """The parents a managed table's family recorded beside the live ones; {} when it records none.

    A parent in the table's own family is its previous output, read to carry rows forward: it is stated as
    ``prior_generation``, never as an input that can lag (roots already published are immutable, so this holds
    whatever the writers record next). ``built_from`` and ``live`` are pins of one kind: family generations for a
    managed parent, whose ``input_table_current`` compares the parent table's own bytes, so a parent family that
    moved for another table does not mark this one stale; the file's sha256 or ETag for an export parent, compared
    with the export receipt this connection matched (``exports``). An unreadable root is stated, never guessed.
    """
    from spicy_regs.sources.publication import PublicationError, table_owner

    owner = table_owner(index, f"{name}.parquet")
    if owner is None:
        return {}
    try:
        parents = _family_parents(R2_BASE_URL, owner[1])
    except (PublicationError, httpx.HTTPError, OSError) as error:
        logger.warning("%s: generation root unavailable (%s)", owner[0], type(error).__name__)
        return {"inputs": None, "inputs_status": "root_unavailable"}
    inputs, prior = [], sorted({parent["artifactDigest"] for parent in parents.values()
                                if parent.get("family") == owner[0]})
    for key, parent in sorted(parents.items()):
        table = key.removesuffix(".parquet")
        if parent.get("family") == owner[0]:
            continue
        if "family" not in parent:
            live_pin, current = _export_pin(exports, table, parent)
            inputs.append({"table": table, "family": None, "built_from": parent.get("etag") or parent.get("sha256"),
                           "live": live_pin, "input_table_current": current})
            continue
        live = table_owner(index, key)
        if live is None:  # no family publishes the parent now: nothing to compare
            live_digest, current = None, None
        elif "sha256" in parent and "sha256" in live[1]["tables"][key]:
            live_digest, current = live[1]["artifactDigest"], parent["sha256"] == live[1]["tables"][key]["sha256"]
        else:
            live_digest = live[1]["artifactDigest"]
            current = parent["artifactDigest"] == live_digest
        inputs.append({"table": table, "family": parent["family"],
                       "built_from": parent["artifactDigest"], "live": live_digest, "input_table_current": current})
    lineage: dict[str, Any] = {"prior_generation": prior[0] if len(prior) == 1 else prior} if prior else {}
    if inputs:
        lineage |= {"inputs": inputs, "inputs_current": _all_current([item["input_table_current"] for item in inputs])}
    return lineage


def _snapshot_lineage(index: dict, manifest: Mapping) -> dict[str, Any]:
    """The rulemaking snapshot's sources beside the live tables, and its previous snapshot as ``prior_generation``.

    The manifest records sources per snapshot, and its stages record what they depend on but not what they read,
    so which sources one table read is unknown: they are ``snapshot_inputs``, and a table's ``inputs_current`` is
    true only when every one is live, else null, never false for a source the table may not have read.
    """
    from spicy_regs.sources.publication import table_owner

    recorded = manifest.get("inputs") or {}
    sources = []
    native = {name + ".parquet": value["publication"]
              for name, value in (recorded.get("native_inputs") or {}).items() if "publication" in value}
    for key, source in sorted({**(recorded.get("sources") or {}), **native}.items()):
        if key in native:
            from spicy_regs.sources.publication import table_pin
            live = table_owner(index, key)
            current = table_pin(index, key) if live is not None else None
            field = "sha256" if "sha256" in source else "tableDescriptorDigest"
            sources.append({"table": key.removesuffix(".parquet"), "family": source["family"],
                            "built_from": source["artifactDigest"],
                            "live": current["artifactDigest"] if current is not None else None,
                            "input_table_current": None if current is None else source[field] == current.get(field)})
            continue
        live = table_owner(index, key)
        live_sha = live[1]["tables"][key].get("sha256") if live is not None else None
        built_from = f"sha256:{source['sha256']}"
        sources.append({"table": key.removesuffix(".parquet"), "built_from": built_from, "live": live_sha,
                        "input_table_current": None if live_sha is None else live_sha == built_from})
    lineage: dict[str, Any] = (
        {"prior_generation": recorded["previous_snapshot_id"]} if recorded.get("previous_snapshot_id") else {})
    if sources:
        lineage |= {"snapshot_inputs": sources,
                    "inputs_current": True if all(item["input_table_current"] for item in sources) else None}
    return lineage


def _export_rows(cursor: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    """``rows``, ``rows_basis`` and ``export_receipt`` for each fixed-URL comments export this connection matched.

    The receipt's count stands only while the object matched it at build, and is labelled as the receipt's: the
    URL is mutable, so a later statement can read a newer export. A moved object states no rows.
    """
    record = _pinned_record(cursor, "_spicy_comments_export")
    exports = {}
    for name, pin in (record["tables"] if record is not None else {}).items():
        matches = pin["matches_object"]
        exports[name] = {
            "rows": pin["rows"] if matches else None,
            "rows_basis": "comments_export_receipt" if matches else "export_receipt_does_not_match_object",
            "export_receipt": {
                "receipt_sha256": pin["receipt_sha256"], "sha256": pin["sha256"], "etag": pin["etag"],
                # An Iceberg snapshot id passes 2**53, which a JavaScript client's JSON number rounds: stated as text.
                "bytes": pin["bytes"], "catalog_snapshot_id": str(pin["source"]["snapshot_id"]),
            },
        }
    return exports


def _reply_pins(cursor: duckdb.DuckDBPyConnection, publication: dict[str, dict], names: list[str]) -> dict[str, dict]:
    """Each named table's pin with the facts a reply states instead of prose that decays.

    ``rows`` is the pinned index's or snapshot manifest's count, a managed
    generation adds ``published_at`` and its basis (when the publisher moved
    the pointer to it; never when the data was read) and what its root
    records (:func:`_input_lineage`), a snapshot table adds its manifest's
    ``run_id``, ``asserted_at`` and sources (:func:`_snapshot_lineage`), a
    comments export adds its receipt's count with ``rows_basis``
    (:func:`_export_rows`), and ``coverage`` is the dictionary's coverage kind. They join the pin only here:
    derived views embed :func:`_publication_status` pins in provenance columns
    and candidate identities, which these facts must not move.
    """
    index, rulemaking, relationships = (
        _connection_index(cursor), _connection_rulemaking(cursor), _connection_relationships(cursor)
    )
    exports = _export_rows(cursor)
    metadata = _selected_table_metadata(index)
    pins = {}
    for name in names:
        pin = dict(publication[name])
        if pin["status"] in ("managed_generation", "managed_download"):
            pin |= {"rows": _pinned_rows(index, rulemaking, name), **_published_at(index, name)}
            # A local download holds no generation roots; the remote root is read on first use, never at build.
            pin |= (_input_lineage(index, name, exports) if pin["status"] == "managed_generation"
                    else {"inputs": None, "inputs_status": "root_unavailable"})
        elif pin["status"] == "managed_receipts":
            pin["rows"] = sum(entry["rows"] for entry in pin["families"].values())
        elif pin["status"] == "rulemaking_snapshot":
            manifest = rulemaking["manifest"]
            pin |= {"rows": rulemaking["tables"][f"{name}.parquet"]["rows"],
                    "run_id": manifest.get("run_id"), "asserted_at": manifest.get("asserted_at"),
                    **_snapshot_lineage(index, manifest)}
        elif name in exports:
            pin |= exports[name]
        declared = metadata.get(name) or relationships.get(name, {}).get("metadata", {})
        # A DocSpec snapshot does not establish the dictionary's population coverage.
        pins[name] = {**pin, "coverage": None if pin["status"] == "docspec_immutable_export" else declared.get("kind")}
    return pins


def _query_reply_pins(pins: dict[str, dict], relationships: dict) -> dict[str, dict]:
    """Project selected query evidence, never the pins used for admission or row identities.

    Full descriptors remain in describe_table. A view's interpretation limits
    come from its registered meaning, even when SQL projects only a value.
    """
    result = {}
    for name, pin in pins.items():
        if "release_compatibility" not in pin:
            result[name] = pin
            continue
        result[name] = deepcopy({
            **{key: pin[key] for key in ("status", "rule_version", "dependencies", "input_publications", "coverage")},
            "release_compatibility": release_summary(pin["release_compatibility"], QUERY_RELEASE_FIELDS),
            "meaning": relationships[name]["metadata"]["summary"],
            "details": "Call describe_table for full release evidence. Compare receipt, SQL, input and evidence pins "
                       "after refresh; a later description may name a different release.",
        })
    return result


# One connection serves every tool call. Building it reads each published
# table's Parquet footer over HTTPS (464 requests and 43 s against r2.dev on
# 2026-09-28); a query on it takes milliseconds to seconds. Each call runs on its
# own worker thread and its own ``cursor()``, DuckDB's way to run overlapping
# statements on one connection, so the statement-timeout interrupt below hits
# only that cursor.
#
# Refresh: the publication index and the rulemaking pointer name immutable URLs,
# so a remote connection stays right until one of them moves. Past the TTL the
# first caller re-reads both (three small GETs) and rebuilds only when they
# moved. Explicit discovery or a missing declared table can check earlier,
# subject to the shared cooldown below. A local connection is rebuilt, since its ``current`` link and member
# signatures are re-read at build. A rebuild is a new DuckDB instance: the
# security settings lock the allowed URLs, so a locked instance cannot admit a
# new generation's views. Other callers keep the current connection meanwhile,
# and a failed refresh keeps it until another eligible check; only a cold start raises.
# The old connection is dropped, never closed: a cursor still mid-query keeps it
# alive until it drains.
_CONNECTION_TTL_SECONDS = float(os.environ.get("SPICY_REGS_CONNECTION_TTL", "300"))
_connection_lock = threading.Lock()
_cached_connection: duckdb.DuckDBPyConnection | None = None
_cached_connection_at = 0.0
_refreshing = False


_discovery_lock = threading.Lock()
_cached_discovery_connection = None
_cached_discovery_at = 0.0
_receipt_lookup_lock = threading.Lock()
_cached_receipt_connection = None
_cached_receipt_at = 0.0
# Explicit discovery and missing declared tables may check before the normal
# TTL. Both caches share one refresh owner and cooldown, including failures.
_EARLY_REFRESH_COOLDOWN_SECONDS = 5.0
_pointer_refresh_lock = threading.Lock()
_last_pointer_refresh_at: float | None = None


@contextmanager
def _bounded_lock(lock):
    while not lock.acquire(timeout=.05):
        checkpoint()
    try:
        checkpoint()
        yield
    finally:
        lock.release()


def _refresh_discovery(current, at):
    checkpoint()
    if current is None:
        return _build_connection(), _monotonic(), None
    timeout = None
    if _claim_pointer_refresh(current, at):
        try:
            current = _refreshed(current)
        except TimeoutError as exc:
            timeout = exc
        except Exception:
            logger.exception("discovery refresh failed; keeping its pinned schemas")
        finally:
            at = _finish_pointer_refresh()
    return current, at, timeout


def _get_discovery_connection():
    """Keep catalog metadata separate from receipt lookups that can read subject rows."""
    global _cached_discovery_connection, _cached_discovery_at, _cached_receipt_connection, _cached_receipt_at
    if _physical_receipt_lookup.get():
        with _bounded_lock(_receipt_lookup_lock):
            current, _cached_receipt_at, timeout = _refresh_discovery(_cached_receipt_connection, _cached_receipt_at)
            _cached_receipt_connection = current
    else:
        with _bounded_lock(_discovery_lock):
            current, _cached_discovery_at, timeout = _refresh_discovery(_cached_discovery_connection, _cached_discovery_at)
            _cached_discovery_connection = current
    if timeout is not None:
        raise timeout
    return current


def _claim_pointer_refresh(current, cached_at: float) -> bool:
    """Admit one pointer check; arbitrary SQL names never make an early check eligible."""
    now = _monotonic()
    expired = now - cached_at >= _CONNECTION_TTL_SECONDS
    if not expired:
        requested = _requested_relations.get()
        if requested is None or not requested and not _discovery_only.get():
            return False
        if not (_discovery_only.get() and not requested):
            with current.cursor() as cursor:
                relationships = _connection_relationships(cursor)
                # Deferred/unsupported relationships already have their own
                # admission/refusal path; a pointer poll cannot repair them.
                known = {*TABLES, "etl_receipts"} | {
                    name for name, entry in relationships.items() if entry["status"] == "unavailable"
                }
                present = set(_available_tables(cursor)) if _discovery_only.get() else _registered_tables(cursor)
                if not (requested & known) - present:
                    return False
    if not _pointer_refresh_lock.acquire(blocking=False):
        return False
    if not expired and (_last_pointer_refresh_at is not None
                        and now - _last_pointer_refresh_at < _EARLY_REFRESH_COOLDOWN_SECONDS):
        _pointer_refresh_lock.release()
        return False
    return True


def _finish_pointer_refresh() -> float:
    global _last_pointer_refresh_at
    _last_pointer_refresh_at = _monotonic()
    _pointer_refresh_lock.release()
    return _last_pointer_refresh_at


def _expand_query_connection(current: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Rebuild locked connections with additional trusted views using the same pinned publication."""
    with current.cursor() as cursor:
        registry = _connection_relationships(cursor)
        publication = _pinned_publication(cursor)
        local = _connection_local_selection(cursor)
        release = _pinned_record(cursor, '_spicy_fec_release')
        scope = _pinned_record(cursor, "_spicy_query_scope")
    pending = {name for name, entry in registry.items() if entry['status'] == 'deferred'}
    requested = _requested_relations.get()
    missing_sources = scope is not None and (not requested or not requested <= set(scope["relations"]))
    if not missing_sources and not (pending if requested is None else pending & requested):
        return current
    if DATA_DIR is not None and local is None:
        raise RuntimeError('Local relationship expansion requires a pinned selection; refresh the connection.')
    installed = {name for name, entry in registry.items() if entry['status'] in ('available', 'unsupported')}
    if scope is not None:
        installed.update(scope["relations"])
    if local is not None:
        from spicy_regs.local_data import assert_local_members_unchanged

        assert_local_members_unchanged(local['signatures'])
    token = _requested_relations.set(None if not requested else installed | requested)
    try:
        replacement = _build_connection(publication)
        try:
            if (_pinned_publication(replacement) != publication
                    or _connection_local_selection(replacement) != local
                    or _pinned_record(replacement, '_spicy_fec_release') != release):
                raise RuntimeError('Selected publication or release changed during relationship expansion; refresh the connection.')
        except BaseException:
            replacement.close()
            raise
        return replacement
    finally:
        _requested_relations.reset(token)


@_diagnostic_phase('connection_access')
def _get_connection() -> duckdb.DuckDBPyConnection:
    """Return the shared connection, refreshing at expiry or for a bounded missing-table check.

    Callers run their statements on ``.cursor()`` of the returned connection, so
    concurrent calls do not share a statement and a timeout interrupt stays
    scoped to one call.
    """
    if _discovery_only.get():
        return _get_discovery_connection()
    global _cached_connection, _cached_connection_at, _refreshing
    with _diagnostic_phase("connection_lock"), _bounded_lock(_connection_lock):
        if _cached_connection is None:  # cold start: concurrent callers wait for this one build
            _cached_connection, _cached_connection_at = _build_connection(), _monotonic()
            return _cached_connection
        if _refreshing:
            # An in-flight refresh owns publication of its replacement. A caller
            # needing more views gets its own pinned replacement, never mutates
            # the locked connection or overwrites the refresh's cache entry.
            return _expand_query_connection(_cached_connection)
        expired = _monotonic() - _cached_connection_at >= _CONNECTION_TTL_SECONDS
        if not expired:
            # Additional scoped views are admitted from the existing pins
            # before deciding whether a missing table warrants a pointer read.
            _cached_connection = _expand_query_connection(_cached_connection)
        if not _claim_pointer_refresh(_cached_connection, _cached_connection_at):
            if expired:
                # Discovery may own the shared pointer check; query admission
                # can still expand from this cache's existing snapshot.
                _cached_connection = _expand_query_connection(_cached_connection)
            return _cached_connection
        current, _refreshing = _cached_connection, True
    replacement = current
    try:
        replacement = _expand_query_connection(_refreshed(current))
    except TimeoutError:
        raise
    except Exception:
        logger.exception("connection refresh failed; serving the pinned connection until the next eligible check")
    finally:
        with _diagnostic_phase("connection_lock"), _connection_lock:
            _cached_connection, _cached_connection_at, _refreshing = replacement, _finish_pointer_refresh(), False
    return replacement


def _refreshed(current: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """``current`` while its remote pointers have not moved; otherwise a new connection."""
    if DATA_DIR is not None:
        selected = _connection_local_selection(current)
        if selected is not None and selected.get("docspec"):
            from spicy_regs.docspec_exports import binding_unchanged
            from spicy_regs.local_data import assert_local_members_unchanged

            configured = os.environ.get("SPICY_REGS_DOCSPEC_EXPORTS")
            if configured == selected["docspec_configuration"]["path"] and binding_unchanged(selected["docspec_configuration"]):
                assert_local_members_unchanged(selected["signatures"])
                return current
        return _build_connection()
    publication = _read_publication()
    if publication != _pinned_publication(current):
        return _build_connection(publication)
    selected = _fec_release_configuration(current, _publication_status(current)["publication"])
    if selected != _pinned_record(current, "_spicy_fec_release"):
        return _build_connection(publication)
    return current


def _reset_connection_cache() -> None:
    """Forget the cached connection so the next call rebuilds. For tests."""
    global _cached_connection, _cached_connection_at, _refreshing, _cached_discovery_connection, _cached_discovery_at
    global _cached_receipt_connection, _cached_receipt_at
    global _last_pointer_refresh_at
    with _connection_lock:
        _cached_connection, _cached_connection_at, _refreshing = None, 0.0, False
    with _discovery_lock:
        _cached_discovery_connection, _cached_discovery_at = None, 0.0
    with _receipt_lookup_lock:
        _cached_receipt_connection, _cached_receipt_at = None, 0.0
    _last_pointer_refresh_at = None


@contextmanager
def _statement_timeout(cursor: duckdb.DuckDBPyConnection) -> Iterator[None]:
    """Bound one cursor's statement runtime, raising TimeoutError when its timer trips.

    Local member signatures are re-checked before and after the statement; with
    no configured timeout this is only the signature check.
    """
    if current_bound() is not None:
        with bounded_connection(cursor):
            local = _connection_local_selection(cursor) if DATA_DIR is not None else None
            if local is not None:
                from spicy_regs.local_data import assert_local_members_unchanged
                assert_local_members_unchanged(local["signatures"])
            yield
            if local is not None:
                assert_local_members_unchanged(local["signatures"])
        return
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    if local is not None:
        from spicy_regs.local_data import assert_local_members_unchanged

        assert_local_members_unchanged(local["signatures"])
    if STATEMENT_TIMEOUT_SECONDS is None:
        yield
        if local is not None:
            assert_local_members_unchanged(local["signatures"])
        return
    tripped = threading.Event()

    def _interrupt() -> None:
        tripped.set()
        # Interrupt only this request's cursor, not the shared connection —
        # sibling requests run on their own cursors and must not be cancelled.
        cursor.interrupt()

    timer = threading.Timer(STATEMENT_TIMEOUT_SECONDS, _interrupt)
    timer.start()
    try:
        yield
        if local is not None:
            assert_local_members_unchanged(local["signatures"])
    except duckdb.InterruptException as exc:
        if tripped.is_set():
            raise TimeoutError(f"Query exceeded the {STATEMENT_TIMEOUT} statement timeout") from exc
        raise
    finally:
        timer.cancel()


@lru_cache(maxsize=1)
@_diagnostic_phase("dictionary_metadata")
def _table_metadata() -> dict[str, dict[str, Any]]:
    """Generated dictionary resource; requires neither spicy-docs nor a checkout."""
    return json.loads(files("spicy_regs").joinpath("table_metadata.json").read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _retained_fec_discovery_metadata() -> dict:
    """Discovery labels retained from the dictionary that served the old FEC generation."""
    return json.loads(files("spicy_regs").joinpath("fec_retained_discovery_metadata.json").read_text(encoding="utf-8"))


def _selected_table_metadata(index: Mapping) -> dict[str, dict[str, Any]]:
    """Use historical labels only for their exact selected generation and member."""
    metadata = _table_metadata()
    family = index["families"].get("fec-query")
    if family is None:
        return metadata
    retained = _retained_fec_discovery_metadata()
    if family.get("artifactDigest") != retained["generation"]:
        return metadata
    historical = {}
    for name, entry in retained["tables"].items():
        descriptor = family["tables"].get(name + ".parquet")
        if descriptor and "members" not in descriptor and all(
            descriptor.get(key) == value for key, value in entry["member"].items()
        ):
            historical[name] = {"table": name, "columns": [], **entry["metadata"]}
    return metadata | historical


def _lineage_meanings(lineage: Mapping[str, Sequence[str]], *, cursor: duckdb.DuckDBPyConnection | None = None) -> dict[str, str]:
    """Dictionary meanings for the view columns that project a dependency table's column unchanged.

    ``lineage`` is the view's ``column_lineage`` (``relationship_views.lineage``),
    read from its parse tree when it was bound: a computed column has no entry and
    so inherits nothing, whatever its name.
    """
    from spicy_regs.fec_receipt_adapter import processing_declarations

    dictionary = _table_metadata()
    processing = processing_declarations()
    meanings: dict[str, str] = {}
    for column, (table, source) in lineage.items():
        original = table.removeprefix("_spicy_fec_processing_").removeprefix("_spicy_fec_current_")
        if table != original and original not in processing:
            continue
        entry = dictionary.get(original, {})
        declared = {c["column_name"]: c.get("description") for c in entry.get("columns", [])}
        legacy = _legacy_column_meanings(cursor, original, entry) if cursor is not None else {}
        meaning = legacy.get(source) or declared.get(source) or processing.get(original, {}).get("descriptions", {}).get(source)
        if meaning:
            meanings[column] = meaning
    return meanings


def _legacy_column_meanings(cursor: duckdb.DuckDBPyConnection, table: str, entry: Mapping) -> dict[str, str]:
    """Historical meanings for actual columns of a selected pre-native generation."""
    from spicy_regs.sources.publication import table_owner

    local = _connection_local_selection(cursor)
    if local is not None and table in local.get("native", {}):
        return {}
    owner = table_owner(_connection_index(cursor), f"{table}.parquet")
    if owner is None or "etlReceipts" in owner[1]:
        return {}
    return dict(entry.get("legacy_column_descriptions", {}))


#: What ``describe_table`` leaves out unless asked with ``detail=true``: the measured records that
#: made bill_versions' reply 24,600 bytes, repeated for every table of a ledger family (round 3, S3).
#: A qualified FEC view also leaves out its release record's inventories (``RELEASE_INVENTORIES``): its
#: dependency's whole storage descriptor and the acceptance receipts were 13,146 of 21,054 characters
#: in fec_receipts_net_receipts_decision's reply (round 4, S4). A table with receipts also leaves out its receipt
#: fields' meanings (:data:`RECEIPT_FIELD_MEANINGS`): names are enough to ask ``read_receipt_fields``, which states
#: the meaning of each field it returns.
DESCRIBE_DETAIL = ("joins[].measurement", "qualification.ledger_statements")
RECEIPT_FIELD_MEANINGS = "receipt_field_meanings"
#: A table's publisher-documented column values and labels (generated from ``source_domains``), also detail-only:
#: lobbying_activities' 79 issue labels and fec_committee_history's 95 party labels would crowd the compact reply.
DOCUMENTED_VALUES = "documented_values"
#: Policy-version column inventories repeat names already given in columns/schema_differences.
#: Keep their version grouping available with detail=true.
DESCRIBE_METADATA_DETAIL = (DOCUMENTED_VALUES, "later_policy_columns")


def _reply_text(result: Any) -> tuple[Any, str]:
    """A reply's JSON value and its compact JSON text: what the client is sent, and what the budget counts."""
    structured = pydantic_core.to_jsonable_python(result, fallback=str)
    return structured, pydantic_core.to_json(structured).decode()


def _bounded_cells(rows: list[list[Any]], columns: Sequence[str], max_chars: int | None) -> list[dict[str, Any]]:
    """Cut each text, list or struct cell longer than ``max_chars`` to its first ``max_chars`` characters, in place.

    A list or struct cell is measured and cut as its compact JSON text. Returns
    one record per cut cell with the cell's full length, so a shortened value is
    never mistaken for the whole; ``None`` cuts nothing.
    """
    cut: list[dict[str, Any]] = []
    if max_chars is None:
        return cut
    for index, row in enumerate(rows):
        for position, value in enumerate(row):
            if isinstance(value, (str, list, dict)):
                text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), ensure_ascii=False)
                if len(text) > max_chars:
                    row[position] = text[:max_chars]
                    cut.append({"row": index, "column": columns[position], "chars": len(text)})
    return cut


def _refuse_oversized_rows(reply: dict[str, Any]) -> None:
    """Refuse a query reply past REPLY_CHARS, naming how many leading rows fit and how to ask for them again.

    Rows are measured one by one only on this path; a whole reply is measured once. The rows that fit leave room
    for the clause the caller is asked to add (RE_ASK_CHARS), since the reply echoes the statement.
    """
    size = len(_reply_text(reply)[1])
    if size <= REPLY_CHARS:
        return
    rows, columns = reply["rows"], reply["columns"]
    room = REPLY_CHARS - RE_ASK_CHARS - len(_reply_text({**reply, "rows": []})[1]) + 1  # the first row has no comma
    fit = 0
    for row in rows:
        room -= len(_reply_text(row)[1]) + 1
        if room < 0:
            break
        fit += 1
    widths = [sum(len(_reply_text(row[position])[1]) for row in rows) for position in range(len(columns))]
    widest = max(range(len(columns)), key=widths.__getitem__)
    cells = (f"set max_cell_chars to cut long cells ({columns[widest]} holds "
             f"{round(100 * widths[widest] / max(sum(widths), 1))}% of the rows' characters), or select fewer columns")
    if not fit:
        raise ValueError(f"This reply would be {size:,} characters, over the {REPLY_CHARS:,}-character reply limit; "
                         f"nothing is returned. Not even the first row fits: {cells}.")
    raise ValueError(
        f"This reply would be {size:,} characters, over the {REPLY_CHARS:,}-character reply limit; nothing is "
        f"returned. The first {fit} of its {len(rows)} rows fit. Ask again with ORDER BY a unique key and LIMIT "
        f"{fit}, then LIMIT {fit} OFFSET {fit} for the next page (compare the publication pins between pages); or "
        f"{cells}.")


def _ledger_index(record: dict) -> tuple[dict, dict[str, list[dict]]]:
    """A qualification record and its ledger rows by table."""
    rows: dict[str, list[dict]] = {}
    for row in record["rows"]:
        for table in row["tables"]:
            rows.setdefault(table, []).append(row)
    return record, rows


@lru_cache(maxsize=1)
def _ledger() -> tuple[dict, dict[str, list[dict]]]:
    """The bundled qualification record (``output_ledger``), read and indexed once per process."""
    return _ledger_index(json.loads(files("spicy_regs").joinpath("table_qualification.json").read_text("utf-8")))


@lru_cache(maxsize=1)
def _joins() -> dict:
    """The bundled join declarations (``table_joins``), read once per process."""
    return json.loads(files("spicy_regs").joinpath("table_joins.json").read_text("utf-8"))


def _table_joins(table: str, *, measurements: bool) -> dict:
    """The declared joins where ``table`` is the child or the parent, with their baseline.

    Detailed outgoing joins retain complete measurements. Incoming joins retain
    baseline counts and declared cardinality, with an explicit reference to the
    child's detailed description for measurement evidence. Without measurements, every
    join keeps its kind and baseline counts. Incoming groups factor the parent
    and a shared complete parent key; empty reasons and null floors are omitted.
    Nonempty reasons and measured floors, including zero, remain exact.
    """
    record = _joins()
    incoming = [join for join in record["joins"] if join["parent"] == table]
    parent_columns = incoming[0]["parent_columns"] if incoming and all(
        join["parent_columns"] == incoming[0]["parent_columns"] for join in incoming
    ) else None

    def shaped(join: dict, *, incoming: bool = False) -> dict:
        shaped = {key: value for key, value in join.items() if measurements or key != "measurement"}
        if not incoming:
            return shaped
        # The enclosing group names the parent once. Factor its columns only
        # when every incoming declaration uses the same complete key.
        shaped.pop("parent")
        if parent_columns is not None:
            shaped.pop("parent_columns")
        if not shaped.get("reason"):
            shaped.pop("reason", None)
        if shaped.get("floor_pct") is None:
            shaped.pop("floor_pct", None)
        # Missing optional fields retain their defaults; all keys, scope reasons,
        # baseline counts and measured floors stay present in both directions.
        if shaped.get("measured_via") is None:
            shaped.pop("measured_via", None)
        if shaped.get("expected_cardinality") == "unspecified":
            shaped.pop("expected_cardinality")
        if measurements:
            if join.get("measurement"):
                shaped["measurement"] = {"status": "see_child_description", "tool": "describe_table",
                                         "arguments": {"table": join["child"], "detail": True}}
            else:
                shaped.pop("measurement", None)
        return shaped

    return {
        "basis": record["basis"],
        "baseline": record["baseline"],
        "outgoing": [shaped(join) for join in record["joins"] if join["child"] == table],
        "incoming_parent": table,
        **({"incoming_parent_columns": parent_columns} if parent_columns is not None else {}),
        "incoming": [shaped(join, incoming=True) for join in incoming],
    }


def _qualification(
    cursor: duckdb.DuckDBPyConnection, tables: list[str], *, statements: bool
) -> tuple[dict, dict[str, dict] | None]:
    """The ledger's scope fields and each table's audit beside its live pin; no tables for another publisher.

    Live pins come from this connection's publication snapshot: the family
    artifact digest, or the table digest for a base object's ``table`` pin,
    and for a rulemaking table the ``snapshot`` its pointer named. A table
    neither pointer names has no live pin to compare.
    """
    record, rows = _ledger()
    scope = {"ledger": record["ledger"], "ledger_destination": record["destination"], "basis": record["basis"]}
    if DATA_DIR is not None or R2_BASE_URL != record["destination"]:
        reads = str(DATA_DIR) if DATA_DIR is not None else R2_BASE_URL
        reason = f"This server reads {reads}; the ledger records audits only for {record['destination']}."
        return {**scope, "status": "unknown_for_publisher", "reason": reason}, None
    # A split table has no single table digest, so only its artifact pin can match an audit.
    live = {
        key.removesuffix(".parquet"): {"artifact": entry["artifactDigest"][7:15],
                                       **({"table": table["sha256"][7:15]} if "sha256" in table else {})}
        for entry in _connection_index(cursor)["families"].values()
        for key, table in entry["tables"].items()
    }
    rulemaking = _connection_rulemaking(cursor)
    for key in rulemaking["tables"]:
        live.setdefault(key.removesuffix(".parquet"), {"snapshot": rulemaking["snapshot_id"][: len("snapshot_") + 8]})
    return scope, {name: _table_qualification(rows.get(name, []), live.get(name, {}), statements) for name in tables}


def _table_qualification(rows: list[dict], live: dict[str, str], statements: bool) -> dict:
    """The audit matching the live pin, else the ledger's latest, as separate fields; never one verified flag."""
    audits = [audit for row in rows for audit in row["audits"]]
    result: dict[str, Any]
    if not audits:
        result = {
            "status": "no_audit_recorded" if rows else "not_in_ledger",
            "live_pin": live.get("artifact", live.get("snapshot")),
        }
    else:
        matched = [audit for audit in audits if live.get(audit["pin_kind"]) == audit["pin"]]
        audit = max(matched or audits, key=lambda item: item["date"])
        live_pin = live.get(audit["pin_kind"])
        if matched:
            generation = "current generation audited"
        elif live_pin:
            generation = "newer generation, not yet audited"
        else:
            generation = "live generation not pinned by the publication index; cannot compare"
        result = {
            "status": "recorded",
            "generation": generation,
            "live_pin": live_pin,
            "ledger_pin": audit["pin"],
            "pin_kind": audit["pin_kind"],
            "ledger_date": audit["date"],
            "ledger_disposition": audit["disposition"],
        }
    if rows:
        result["ledger_tasks"] = list(dict.fromkeys(row["task"] for row in rows))
    if statements and rows:
        result["ledger_statements"] = [row["statement"] for row in rows]
    return result


def _archive_summary(archive: dict | None, available: list[str]) -> dict[str, Any] | None:
    """list_sources' archive group: the pinned release, each view's file pin and purpose, and recipe availability."""
    if archive is None or archive["status"] != "bound":
        return archive
    return {
        **{key: archive[key] for key in ("status", "release_id", "origin", "pins_url", "trust", "name_map")},
        "views": [{"table": name, "available": name in available, "purpose": view.get("purpose"), "rows": view["rows"]}
                  for name, view in archive["views"].items()],
        "recipes": [{"id": item["id"], "title": item["title"], "available_here": item["available_here"]}
                    for item in archive["recipes"]],
        "details": "describe_table on an archive_* view gives columns, identity, notes, relationships and recipes "
                   "as published; recipe SQL names release tables, which name_map maps to these views.",
    }


def _describe_archive_view(cursor, archive: dict, table: str, *, available: bool) -> dict[str, Any]:
    """describe_table for one archive view: the published file entry, its relationships and recipes, as published."""
    from spicy_regs.archive_release import relationship_tables

    view = archive["views"][table]
    stem = view["file"].removesuffix(".parquet")
    meanings = {column["name"]: column.get("meaning")
                for column in (*view.get("columns", []), *view.get("virtual_columns", []))}
    actual = cursor.execute(f'DESCRIBE "{table}"').fetchall() if available else []
    return {
        "table": table,
        **_source_details(cursor),
        "available": available,
        "publication": {"status": "archive_release", "release_id": archive["release_id"], "origin": archive["origin"],
                        "pins_url": archive["pins_url"], "trust": archive["trust"],
                        **{key: view[key] for key in ("file", "sha256", "size", "rows")}},
        "metadata": {key: view[key] for key in ("purpose", "row_meaning", "identity", "gaps", "notes") if key in view},
        "name_map": archive["name_map"],
        "relationships": [item for item in archive["relationships"] if stem in relationship_tables(item)],
        "recipes": [item for item in archive["recipes"] if stem in item["requires"]],
        "columns": ([{"column_name": name, "column_type": dtype, "description": meanings.get(name)}
                     for name, dtype, *_ in actual] if available else
                    [{"column_name": name, "column_type": None, "description": meaning} for name, meaning in meanings.items()]),
    }


def _registered_tables(cursor: duckdb.DuckDBPyConnection) -> set[str]:
    """Actual main-schema tables/views, without reading data or publication metadata."""
    rows = cursor.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_catalog = current_database()"
    ).fetchall()
    return {row[0] for row in rows}


def _available_tables(cursor: duckdb.DuckDBPyConnection) -> list[str]:
    """Registered relations and, for remote discovery, selected publication entries."""
    registered = _registered_tables(cursor)
    managed = [
        key.removesuffix(".parquet")
        for entry in _connection_index(cursor)["families"].values()
        for key in entry["tables"]
    ]
    snapshot = [key.removesuffix(".parquet") for key in _connection_rulemaking(cursor)["tables"]]
    local = _connection_local_selection(cursor) if DATA_DIR is not None else None
    selected = local["selected_tables"] if local is not None else []
    if _DISCOVERY_CATALOG in registered:
        catalog = _pinned_record(cursor, _DISCOVERY_CATALOG)
        if catalog is None:
            raise RuntimeError("Missing selected discovery metadata")
        registered.update(managed)
        if catalog["receipt_columns"] is not None:
            registered.add("etl_receipts")
    relationships = _connection_relationships(cursor)
    archive = (_pinned_record(cursor, "_spicy_archive") or {}).get("views", {})
    return [
        name for name in dict.fromkeys((*TABLES, *managed, *snapshot, *selected, *relationships, "etl_receipts", *archive))
        if name in registered
    ]


def _document_kind(requested: str) -> str:
    """The supported kind ``requested`` names, compared case-insensitively; any other kind is refused.

    The refusal names every supported kind and the closest one: the kinds whose
    documents a named table holds (``house_activity_reports`` holds
    ``govinfo_package``), else the nearest spelling.
    """
    kind = requested.lower()
    if kind in SOURCE_TABLES:
        return kind
    closest = [name for name, table in SOURCE_TABLES.items() if table in (kind, kind + "s")]
    closest = closest or difflib.get_close_matches(kind, DOCUMENT_KINDS, n=1)
    hint = (
        f" The closest supported kind is {closest[0]!r}." if len(closest) == 1
        else f" The closest supported kinds are {', '.join(map(repr, closest))}." if closest else ""
    )
    raise ValueError(
        f"Unsupported document_kind {requested!r}.{hint} Supported kinds: {', '.join(DOCUMENT_KINDS)}; "
        "no other kind has held citation rows."
    )


#: The held-citations pipeline's read record: one row per read of a held field's text, published beside
#: document_citations (implementer C, round 5). Until it is published a held field has no read record.
HELD_FIELD_READS = "document_citation_reads"


def _source_read(
    cursor: duckdb.DuckDBPyConnection, tables: list[str], kind: str, key: str, *, cited: bool
) -> tuple[dict[str, Any], str | None]:
    """Whether ``kind``'s table holds ``key`` and records reading it, and the held text's digest.

    ``cited`` says whether document_citations holds any row for the document.
    With rows, the status names the digest they are checked against (``read``,
    ``missing_digest``, ``ambiguous``). Without, it says whether the table holds
    the document (``not_held``) and records a read that found nothing
    (``read_none_found``) or none (``not_read``). A print kind's table records
    its read in the row: the text digest, ``pages_read``, ``rule_set_version``
    and the ``citation_rows`` it produced. A held field's read is recorded in
    :data:`HELD_FIELD_READS`, when published: the latest read of the field's
    current text, with or without a rule set.
    """
    from spicy_regs.citation_sources import TEXT_SOURCES, source_digests

    from spicy_regs.citation_receipts import selected_citation_inputs

    selected = selected_citation_inputs(cursor)
    inputs = selected["tables"]
    parent = SOURCE_TABLES[kind]
    if parent not in tables:
        return {"table": parent, "status": "unavailable"}, None
    if parent not in selected["selected_sources"]:
        raise ValueError(f"Citation source {parent} requires selected native ETL receipts")
    try:
        if kind not in TEXT_SOURCES:
            values = cursor.execute(
                "SELECT DISTINCT text_sha256, pages_read IS NOT NULL AND rule_set_version IS NOT NULL, "
                f'CAST(citation_rows AS BIGINT) FROM "{inputs[parent]}" WHERE package_id = ? LIMIT 2',
                [key],
            ).fetchall()
        else:
            values = [(digest, False, None) for (digest,) in source_digests(
                cursor, kind, key, table=selected["sources"][parent],
                digest_field="sha256_" + TEXT_SOURCES[kind].field)]
            if len(values) == 1 and values[0][0] and not cited and HELD_FIELD_READS in inputs:
                # Every read published so far states no rule set and no time (round 6, 13 of 13): the dictionary
                # calls that a read recorded before the table existed. An undated read stating rows outranks one
                # stating none, so a disagreement refuses rather than reading as "none found".
                read = cursor.execute(
                    f'SELECT CAST(citation_rows AS BIGINT) FROM "{inputs[HELD_FIELD_READS]}" WHERE document_kind = ? '
                    "AND document_key = ? AND text_sha256 = ? "
                    "ORDER BY read_at DESC NULLS LAST, CAST(citation_rows AS BIGINT) DESC LIMIT 1",
                    [kind, key, values[0][0]],
                ).fetchone()
                values = [(values[0][0], read is not None, read[0] if read is not None else None)]
    except duckdb.InterruptException:
        raise
    except duckdb.Error as error:
        return {"table": parent, "status": "read_failure", "error_type": type(error).__name__}, None
    if len(values) != 1:
        return {"table": parent, "status": "ambiguous" if values else "not_held"}, None
    [(digest, recorded, rows)] = values
    if cited:
        return {"table": parent, "status": "read" if digest else "missing_digest"}, digest or None
    read = bool(digest and recorded and rows is not None)
    if read and rows != 0:
        record = parent if kind not in TEXT_SOURCES else HELD_FIELD_READS
        raise ValueError(f"{record} records a read of {key!r} that states {rows} citation rows, but "
                         "document_citations holds none for it: this publication disagrees with itself.")
    return {"table": parent, "status": "read_none_found" if read else "not_read"}, None


def _hoist(rows: list[dict[str, Any]], fields: Sequence[str]) -> tuple[dict[str, Any], list[str]]:
    """Of ``fields``, those every row holds with one value, and those the rows hold with different values.

    A listed field that differs (a document holding two texts, say) stays on
    each row: a stated-once value is never a guess.
    """
    shared: dict[str, Any] = {}
    varying: list[str] = []
    for field in fields:
        values = [row[field] for row in rows if field in row]
        if values and len(values) == len(rows) and all(value == values[0] for value in values[1:]):
            shared[field] = values[0]
        elif values:
            varying.append(field)
    return shared, varying


def _compact_occurrences(occurrences: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """State the fixed per-document and per-kind fields once: a reply projection, like _query_reply_pins.

    :data:`OCCURRENCE_DOCUMENT_FIELDS` go to ``shared`` and
    :data:`OCCURRENCE_KIND_FIELDS` to ``by_cite_kind``, each where every
    occurrence in its scope agrees (else to ``not_hoisted``, left on each
    occurrence); a field of :data:`OCCURRENCE_SAME_AS` leaves an occurrence
    where it equals the field it names (``same_as`` lists those the rows
    hold). cite_kind stays on every occurrence, so ``{**shared,
    **by_cite_kind[cite_kind], **occurrence}`` is the row. ``occurrence_key``,
    a digest of six fields the row states (round 6: a fifth of each
    occurrence), is left out: the queue names occurrences by their spans. The
    resolver's rows and the queue built from them are unchanged.
    """
    same_as = {field: other for field, other in OCCURRENCE_SAME_AS.items() if any(field in row for row in occurrences)}
    rows = [{key: value for key, value in row.items() if key != "occurrence_key"
             and not (key in same_as and value == row.get(same_as[key]))} for row in occurrences]
    shared, not_hoisted = _hoist(rows, OCCURRENCE_DOCUMENT_FIELDS)
    groups: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(row.get("cite_kind"), []).append(row)
    by_kind = {}
    for kind, group in groups.items():
        by_kind[kind], varying = _hoist(group, OCCURRENCE_KIND_FIELDS)
        not_hoisted += [field for field in varying if field not in not_hoisted]
    compact = [{key: value for key, value in row.items()
                if key not in shared and key not in by_kind[row.get("cite_kind")]} for row in rows]
    return compact, {
        "hoisted": {"shared": list(OCCURRENCE_DOCUMENT_FIELDS), "by_cite_kind": list(OCCURRENCE_KIND_FIELDS)},
        "shared": shared, "by_cite_kind": by_kind, "not_hoisted": not_hoisted, "same_as": same_as,
        "meaning": "Each occurrence is {**shared, **by_cite_kind[its cite_kind], **occurrence}; a same_as field it "
                   "lacks equals the field named there. A hoisted field whose values differ on this page is named in "
                   "not_hoisted and stays on each occurrence. occurrence_key, a digest of fields each occurrence "
                   "states, is left out.",
    }


def _compact_queue(queue: dict[str, Any], input_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """The acquisition queue stated against the reply it sits in; nothing is left out that the reply does not state.

    An item names its requesting occurrences by span (``requesting_spans``): each is an occurrence of the reply
    whose cite_kind and target_key are the item's target_kind and normalized_key, which states every field a
    request restated but ``input_snapshot``, stated once (round 6: requests were 14,903 of a 102,942-character
    reply). :data:`QUEUE_ITEM_FIELDS` are stated once and :data:`QUEUE_KIND_FIELDS` once per target kind where
    they agree; a route's ``native_identifier`` leaves it where it equals ``normalized_key``, and the queue's
    ``resolution_coverage`` where it is the reply's ``coverage``. Same projection as :func:`_compact_occurrences`.
    """
    items = []
    for item in queue["items"]:
        compact = {key: value for key, value in item.items() if key != "requesting_occurrences"}
        if item["provider_route"].get("native_identifier") == item["normalized_key"]:
            compact["provider_route"] = {key: value for key, value in item["provider_route"].items()
                                         if key != "native_identifier"}
        items.append({**compact, "requesting_spans": list(dict.fromkeys(
            request["span_start"] for request in item["requesting_occurrences"]))})
    shared, not_hoisted = _hoist(items, QUEUE_ITEM_FIELDS)
    groups: dict[Any, list[dict[str, Any]]] = {}
    for item in items:
        groups.setdefault(item["target_kind"], []).append(item)
    by_kind = {}
    for kind, group in groups.items():
        by_kind[kind], varying = _hoist(group, QUEUE_KIND_FIELDS)
        not_hoisted += [field for field in varying if field not in not_hoisted]
    coverage = {key: value for key, value in queue["coverage"].items() if key != "resolution_coverage"}
    return {**queue, "coverage": coverage, "items": [
        {key: value for key, value in item.items() if key not in shared and key not in by_kind[item["target_kind"]]}
        for item in items
    ], "shared_fields": {
        "hoisted": {"item": list(QUEUE_ITEM_FIELDS), "by_target_kind": list(QUEUE_KIND_FIELDS)},
        "item": shared, "by_target_kind": by_kind, "not_hoisted": not_hoisted, "input_snapshot": dict(input_snapshot),
        "same_as": {"provider_route.native_identifier": "normalized_key", "coverage.resolution_coverage": "coverage"},
        "meaning": "Each item is {**item, **by_target_kind[its target_kind], **its fields}. Each requesting_spans "
                   "entry is the span_start of an occurrence above whose cite_kind and target_key are the item's "
                   "target_kind and normalized_key: that occurrence, with input_snapshot, is the request. A same_as "
                   "field left out equals the field named there (coverage is this reply's).",
    }}


def _refuse_oversized_page(reply: dict[str, Any], page: Callable[[list[dict[str, Any]]], dict[str, Any]],
                           occurrences: list[dict[str, Any]], offset: int) -> None:
    """Refuse a citation page past REPLY_CHARS, naming how many of its first occurrences fit and how to page them.

    A page's size never falls as occurrences are added (each brings its fields and its queue entry, and can only
    stop a field being stated once), so the largest page that fits is found by halving: about seven page builds
    for 100 occurrences, from rows already resolved, with no further read.
    """
    size = len(_reply_text(reply)[1])
    if size <= REPLY_CHARS:
        return
    fit, over = 0, len(occurrences)
    while over - fit > 1:
        middle = (fit + over) // 2
        if len(_reply_text(page(occurrences[:middle]))[1]) <= REPLY_CHARS - RE_ASK_CHARS:
            fit = middle
        else:
            over = middle
    over_limit = f"This page would be {size:,} characters, over the {REPLY_CHARS:,}-character reply limit; nothing is " \
                 "returned."
    if not fit:
        raise ValueError(f"{over_limit} Not even its first occurrence fits; cite_kind selects one kind's occurrences.")
    raise ValueError(
        f"{over_limit} The first {fit} of its {len(occurrences)} occurrences fit: ask again with max_occurrences={fit} "
        f"and offset={offset}, then offset={offset + fit} for the next page (cite_kind selects one kind).")


def _unheld_document(cursor: duckdb.DuckDBPyConnection, kind: str, key: str, parent: str) -> str:
    """The refusal for a document_key that neither ``parent`` nor any citation row of ``kind`` holds.

    It names each held spelling of the key that differs only in case, under any
    kind, read from document_citations alone: a parent such as ``comments`` is
    too large to scan for a case-folded key.
    """
    held = cursor.execute(
        "SELECT DISTINCT document_kind, document_key FROM document_citations "
        "WHERE lower(document_key) = lower(?) ORDER BY ALL LIMIT 3",
        [key],
    ).fetchall()
    hint = "".join(f" Citation rows exist under {held_kind} {held_key!r}." for held_kind, held_key in held)
    return (f"No {parent} row and no {kind} citation row has document_key {key!r}; document keys are exact and "
            f"case-sensitive, and {kind} covers only the documents {parent} holds.{hint}")


def _argument_problem(error: Mapping[str, Any]) -> str:
    """One argument error in plain words: ``sql is required`` or ``max_rows: Input should be ...``."""
    where = ".".join(map(str, error["loc"]))
    return f"{where} is required" if error["type"] == "missing" else f"{where}: {error['msg']}"


class _StrictTool(Tool):
    """A tool that refuses an argument it does not declare and states every argument error in plain words.

    MCPServer's argument model ignores an undeclared argument, so a misspelled ``offest`` ran page 0 with nothing
    said (round 6), and a bound failed in pydantic's own text with a link to its documentation. The refusal names
    each problem, then the arguments the tool takes; the schema says ``additionalProperties: false``.
    """

    async def run(self, arguments: dict[str, Any], context: Any, convert_result: bool = False) -> Any:
        names = sorted(self.parameters["properties"])
        problems = [f"{name} is not an argument" for name in sorted(set(arguments) - set(names))]
        cause = None
        try:
            self.fn_metadata.validate_arguments({key: value for key, value in arguments.items() if key in names})
        except ValidationError as error:
            cause = error  # MCPServer then logs the field names only, never the caller's values
            problems += [_argument_problem(e) for e in error.errors(include_url=False, include_input=False)]
        if problems:
            raise ToolError(f"Error executing tool {self.name}: {'; '.join(problems)}. "
                            f"{self.name} takes {', '.join(names) or 'no arguments'}.") from cause
        return await super().run(arguments, context, convert_result)

    @classmethod
    def strict(cls, fn: Callable[..., Any], description: str) -> Tool:
        """``fn`` as a tool whose schema refuses other arguments and drops pydantic's titles, which repeat each name
        and count against the client's description cap."""
        tool = cls.from_function(fn, description=description)
        schema = {key: value for key, value in tool.parameters.items() if key != "title"}
        schema["properties"] = {name: {key: value for key, value in field.items() if key != "title"}
                                for name, field in schema["properties"].items()}
        return tool.model_copy(update={"parameters": {**schema, "additionalProperties": False}})


def _run_observed_tool(fn, arguments):
    with _diagnostic_phase("tool", fn.__name__):
        return fn(**arguments)


def _tools() -> list[Tool]:
    """Data tools share a worker limiter; connection_status reads bounded phase metadata directly."""
    limiter = anyio.CapacityLimiter(TOOL_CONCURRENCY)
    tools: list[Tool] = []

    def tool(fn: Callable[..., dict[str, Any]]) -> Callable[..., Any]:
        """Register ``fn`` to run on a worker thread, at most TOOL_CONCURRENCY calls at once, off the event loop.

        A failure reaches the caller with its text. MCPServer shows only a
        ToolError's message, and DuckDB's errors and the read-only refusals are
        what a caller needs to correct a query.

        The description is the docstring without its indentation: MCPServer sends
        ``__doc__`` as written, and clients cap descriptions (Claude Code at 2,048
        characters). The reply's text block is the compact JSON of its structured
        content; the SDK's default text is the same object indented, sent twice.
        """

        @functools.wraps(fn)
        async def call(**arguments: Any) -> CallToolResult:
            bound = RequestBound(STATEMENT_TIMEOUT_SECONDS, f"Request exceeded the {STATEMENT_TIMEOUT} timeout, including input admission")
            def run():
                name = getattr(fn, "__name__", None)
                # Receipt lookups admit their selected index themselves. Keep local
                # native row validation, without restoring unrelated remote views.
                remote_lookup = name == "read_receipt_fields" and DATA_DIR is None
                lookup_token = _physical_receipt_lookup.set(remote_lookup)
                token = _discovery_only.set(name in ("list_sources", "describe_table") or remote_lookup)
                requested_token = None
                try:
                    with bound.scope():
                        if name == 'query_sql':
                            # Inspect the unbound parse tree before opening source
                            # views. Never execute caller SQL on this parser.
                            with duckdb.connect() as parser:
                                write_statement = _first_write_statement(parser, arguments['sql'])
                                if write_statement is not None:
                                    raise ValueError(f"query_sql is read-only; refusing {write_statement} statement")
                                requested_token = _requested_relations.set(_tables_named(parser, arguments['sql']))
                        elif name in ('list_sources', 'describe_table'):
                            requested_token = _requested_relations.set({arguments['table']} if name == 'describe_table' else set())
                        return _run_observed_tool(fn, arguments)
                finally:
                    if requested_token is not None:
                        _requested_relations.reset(requested_token)
                    _discovery_only.reset(token)
                    _physical_receipt_lookup.reset(lookup_token)
            try:
                with anyio.fail_after(STATEMENT_TIMEOUT_SECONDS):
                    result = await anyio.to_thread.run_sync(run, limiter=limiter, abandon_on_cancel=True)
            except TimeoutError as exc:
                raise ToolError(bound.label) from exc
            except Exception as exc:
                raise ToolError(str(exc)) from exc
            finally:
                bound.cancel()
            structured, text = _reply_text(result)
            return CallToolResult(content=[TextContent(type="text", text=text)], structured_content=structured)

        tools.append(_StrictTool.strict(call, inspect.cleandoc(fn.__doc__ or "")))
        return call

    @tool
    def lookup_agency(
        namespace: Namespace,
        identifier: Annotated[str, Field(min_length=1, max_length=256)],
        on_date: Annotated[str | None, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")] = None,
    ) -> dict[str, Any]:
        """Look up an exact agency identifier in the pinned reviewed RefSpec mapping.

        Namespaces are regulations.gov:agency (e.g. OPM) and
        federal_register_agency (e.g. 406). Labels are not identifiers.
        Return mapping evidence, publication pins and documented abstentions.
        Parent relationships and succession events are not identity: whether
        two names are one agency is the registry's own judgment, stated in each
        evidence row's reasoning sentence. parent_labels gives each parent's
        name where the registry's rows state one (null where none does).
        REF-072 bridge and event evidence stays separate from REF-038
        candidates. registry_evidence.current_lineage is computed for
        federal_register_agency ids only (a regulations.gov code reads
        not_applicable) and follows the owner policy without dates; on_date
        preserves evidence but cannot establish historical identity.
        No acquisition, new adjudication, or money attribution is performed.
        """
        from spicy_regs.vocabulary_mapping import lookup_agency as lookup

        return lookup(namespace, identifier, on_date=on_date)

    @tool
    def list_sources(subject: Annotated[str | None, Field(min_length=1, max_length=64)] = None) -> dict[str, Any]:
        """List table names by subject. Call without subject, then list_sources(subject).

        Managed names use publication metadata. SQL checks file schemas.
        The default shows table names, unavailable_tables preview, view counts, receipt summary
        and fec_release.status_counts. unavailable_views counts declared views still unloaded.
        The default omits selected empty and sampled tables.
        Explicit subject pages, views, describe_table and query_sql retain access.
        Unassigned means no dictionary subject. Unknown subjects are refused.
        views lists relationship groups, unavailable tables and views, deferred bindings
        and the FEC release. archive gives the archive release. Match generation across pages
        to use the same pinned inputs. A declared subject may have no loaded tables.

        Subject pages give labels, coverage and pinned rows. Coverage kinds are true_range, window,
        sampled, derived, not_a_range and empty. A window or sample covers part of the source history.
        A zero count means this generation publishes no rows. Null means no pinned count.
        Later runs can finish after the pin. Check freshness separately.
        rows_basis comments_export_receipt uses the fixed-URL
        export receipt. export_receipt_does_not_match_object means the file moved and rows is null.
        child_query entries describe response or array-element rows and name source_tables.
        category marks diagnostics, evidence, leads and samples. Generic query_data omits category.

        Deferred views await query binding and have no proven output schema. fec_release.status_counts
        counts compatible and disabled
        views. Raw financial totals need separate qualification. Use describe_table for columns,
        caveats, joins, data pins, audit details and release evidence.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            available = _available_tables(cursor)
        relationships = _connection_relationships(cursor)
        index, rulemaking, exports = _connection_index(cursor), _connection_rulemaking(cursor), _export_rows(cursor)
        metadata = _selected_table_metadata(index)
        local = _connection_local_selection(cursor) if DATA_DIR is not None else None
        # A relationship family's occurrence, pair and field-state views share one summary; list it once.
        views: dict[str, list[str]] = {}
        deferred: dict[tuple[str, str], list[str]] = {}
        for name, entry in relationships.items():
            if entry["status"] == "deferred":
                deferred.setdefault((entry["reason"], entry["metadata"]["query_admission"]), []).append(name)
            if name in available and entry["metadata"].get("view_role") != "child_query":
                views.setdefault(entry["metadata"]["summary"], []).append(name)

        def rows(name: str) -> dict[str, Any]:
            if name == "etl_receipts" and (families := _receipt_families(index, local)):
                return {"rows": sum(entry["rows"] for entry in families.values())}
            export = exports.get(name)
            if export is None:
                return {"rows": _pinned_rows(index, rulemaking, name)}
            return {"rows": export["rows"], "rows_basis": export["rows_basis"]}

        # Grouped by the dictionary's subject, in the order subjects first appear in the display order; a table the
        # dictionary gives no subject yet is in the last group. Two personas could not find the rulemaking tables
        # among 158 listed in one run (round 6, L10).
        subjects: dict[str | None, list[dict[str, Any]]] = {}
        archive = _pinned_record(cursor, "_spicy_archive")
        for name in available:
            if name in (archive or {}).get("views", {}):
                continue
            relationship = relationships.get(name, {})
            view = relationship.get("metadata", {})
            if not relationship or view.get("view_role") == "child_query":
                entry = metadata.get(name, {})
                subjects.setdefault(entry.get("subject", "campaign_finance" if view.get("view_role") == "child_query" else None), []).append(
                    {"table": name, "label": entry.get("label", view.get("label")),
                     "coverage": entry.get("kind", "derived"), **rows(name),
                     **({"category": entry["category"]} if entry.get("category") and entry["category"] != "query_data" else {}),
                     **({"role": "child_query", "source_tables": relationship["dependencies"]}
                        if relationship else {})})
        if None in subjects:
            subjects[None] = subjects.pop(None)
        # Paged by subject (owner decision 2026-10-31, ss-ehe5): the whole catalog reached 39,135 of the 40,000-character
        # reply budget, and every new table added about 111. A page's table entries are exactly the whole reply's were.
        pages = {UNASSIGNED_SUBJECT if name is None else name: name for name in subjects}
        other_pages = ["views", *(["archive"] if archive else [])]
        # A disabled FEC release makes every qualified view unavailable (72 names), so view names stay on their page.
        present = set(available)
        unavailable = dict.fromkeys(name for name in (*TABLES, *relationships)
                                    if name not in present and relationships.get(name, {}).get("status") != "deferred")
        declared = set(TABLES)
        unavailable_views = [name for name in unavailable if name not in declared]
        unavailable_tables = [name for name in unavailable if name in declared]
        identity = {**_source_details(cursor), "generation": _connection_generation(cursor)}
        if _pinned_record(cursor, _DISCOVERY_CATALOG) is not None:
            identity["discovery_basis"] = "Selected publication for managed tables. SQL admission checks their files and schemas."

        if subject is None:
            receipt_families = _receipt_families(index, local)
            visible_subjects = [
                {"subject": UNASSIGNED_SUBJECT if name is None else name, "tables": names}
                for name, tables in subjects.items()
                if (names := [table["table"] for table in tables
                              if table["table"] not in DEFAULT_DISCOVERY_EXCLUDED_TABLES])
            ]
            visible_unavailable = [name for name in unavailable_tables
                                   if name not in DEFAULT_DISCOVERY_EXCLUDED_TABLES]
            # Names only (owner decision 2026-10-31, ss-zt4p): one call shows what each subject holds;
            # its page adds label, coverage and rows. The default's budget is 10,000 characters.
            return {
                **identity,
                "subjects": visible_subjects,
                "unavailable_tables": visible_unavailable[:UNAVAILABLE_TABLES_LISTED],
                "unavailable_table_count": len(visible_unavailable),
                "relationship_views": {"available": sum(len(names) for names in views.values())},
                "unavailable_views": len(unavailable_views),
                "deferred_views": [{"count": len(names), "reason": reason, "query_admission": admission}
                                   for (reason, admission), names in deferred.items()],
                "etl_receipts": {
                    "available": "etl_receipts" in available,
                    "dataset_count": len(metadata.get("etl_receipts", {}).get("datasets", [])),
                    "family_count": len(receipt_families),
                    "details": "describe_table('etl_receipts') gives datasets, columns and, while it is available, each family's pins.",
                    "selection": "Receipt and subject versions must belong to the same selected generation; absence does not mean no processing history.",
                },
                "fec_release": {"status_counts": _fec_release_reply(cursor)["status_counts"]},
                **({"archive": {"status": archive["status"], "views": len(archive.get("views", {}))}} if archive else {}),
                "pages": other_pages,
                "details": "list_sources(subject) takes a subject above or one of pages.",
            }
        # A declared subject with no loaded table is empty here, not unknown (a partial or local server).
        empty = [name for name in dict.fromkeys(entry.get("subject") for entry in metadata.values())
                 if name is not None and name not in pages]
        if subject in pages or subject in empty:
            key = pages.get(subject, subject)
            return {
                **identity,
                "subjects": [{"subject": key, "tables": subjects.get(key, [])}],
                "category_details": "Generic query_data declarations are omitted; absent category can also mean undeclared. describe_table gives declared categories and limits.",
            }
        if subject == "views":
            return {
                **identity,
                "relationship_views": [{"views": names} for names in views.values()],
                "relationship_view_details": "Call describe_table for group labels, full meanings, identities, dependencies and purpose limits.",
                "unavailable_tables": unavailable_tables,
                "unavailable_views": unavailable_views,
                "deferred_views": [{"views": names, "reason": reason, "query_admission": admission}
                                   for (reason, admission), names in deferred.items()],
                "fec_release": _fec_release_reply(cursor),
            }
        if subject == "archive" and archive:
            return {**identity, "archive": _archive_summary(archive, available)}
        raise ValueError(f"Unknown subject '{subject}'. list_sources takes one of: {', '.join([*pages, *empty, *other_pages])}.")

    @tool
    def describe_table(table: str, detail: bool = False) -> dict[str, Any]:
        """Return columns, meanings, identity, coverage and joins.

        The bundled output ledger describes coverage scope. columns gives meanings.
        schema_differences compares managed remote publication columns or loaded
        fields. SQL checks physical schemas. publication pins rows and coverage;
        published_at is a pointer move, not a source read. last_object_write
        bounds that move from below.
        inputs: recorded parents, with built_from beside live. No recorded
        parents does not mean no inputs; reads bypassing the download helper
        are not recorded. input_table_current compares parent bytes even if
        its family moved for another table; inputs_current is false if any lag,
        else null if unknown.
        prior_generation: earlier output; snapshot_inputs: rulemaking sources.
        qualification compares live/audited pins, date and disposition.
        not_in_ledger means absent from the publisher ledger, not unevidenced.
        joins gives outgoing/incoming declarations. detail=true adds outgoing
        measurements and ledger statements; incoming measurements link to the
        child, retaining baselines. incoming_parent names the parent;
        incoming_parent_columns gives its complete key, otherwise parent_columns
        appears per entry. Missing reason means empty; floor_pct, no measured
        floor; expected_cardinality, unspecified; measured_via, measure the child.
        detail=false: omissions in detail.omitted; receipt_fields names
        read_receipt_fields inputs.
        FEC release_compatibility is in publication (relationship if unavailable).
        detail=false retains pins, reasons, generations and receipt count.
        pending_receipt_rows means row admission/output binding is deferred;
        matching pins do not establish execution.
        compatible means captured data, interpretation and consumer match
        the selected release, not current/net money or completeness. Financial
        eligibility applies to its named purpose.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            status = _publication_status(cursor)
        relationships = _connection_relationships(cursor)
        archive = _pinned_record(cursor, "_spicy_archive")
        if archive is not None and table in archive.get("views", {}):
            with _statement_timeout(cursor):
                return _describe_archive_view(cursor, archive, table, available=table in status["tables"])
        if table not in TABLES and table != "etl_receipts" and table not in status["tables"] and table not in relationships:
            known = list(dict.fromkeys((*TABLES, *status["tables"], *relationships)))
            close = difflib.get_close_matches(table, known, n=5, cutoff=0.6)
            hint = f" Close names: {', '.join(close)}." if close else ""
            raise ValueError(
                f"Unknown table '{table}'.{hint} Use list_sources(subject) for loaded tables "
                "or list_sources('views') for unavailable tables."
            )
        with _statement_timeout(cursor):
            available = table in status["tables"]
            catalog = _pinned_record(cursor, _DISCOVERY_CATALOG)
            published_columns = None
            if catalog is not None and available:
                from spicy_regs.sources.publication import table_descriptor

                if table == "etl_receipts":
                    published_columns = catalog["receipt_columns"]
                elif descriptor := table_descriptor(_connection_index(cursor), f"{table}.parquet"):
                    published_columns = descriptor["columns"]
            rows = (published_columns if published_columns is not None else
                    cursor.execute(f'DESCRIBE "{table}"').fetchall() if available else [])
        entry: dict[str, Any]
        if table in relationships:
            from spicy_regs.relationship_views import view_columns

            # A derived view declares its bound schema; it is described here, not at connection build.
            metadata = relationships[table]["metadata"]
            inherited = _lineage_meanings(metadata.get("column_lineage", {}), cursor=cursor)
            entry = {"table": table, **metadata,
                     "columns": view_columns(rows, {**_table_metadata().get(table, {}).get("column_descriptions", {}),
                                                   **metadata.get("column_descriptions", {})}, inherited)}
        else:
            entry = _selected_table_metadata(_connection_index(cursor)).get(table, {"table": table, "columns": []})
        declared = {column["column_name"]: column for column in entry["columns"]}
        actual = {row[0]: row[1] for row in rows}
        legacy_meanings = _legacy_column_meanings(cursor, table, entry) if available else {}
        scope, qualified = _qualification(cursor, [table], statements=detail)
        differences = (
            {
                "missing_columns": [name for name in declared if name not in actual],
                "unexpected_columns": [name for name in actual if name not in declared],
                "type_differences": [
                    {"column": name, "declared": declared[name]["column_type"], "actual": dtype}
                    for name, dtype in actual.items()
                    if name in declared and dtype != declared[name]["column_type"]
                ],
            }
            if available and declared
            else None
        )
        relationship: dict[str, Any] = {
            key: value for key, value in relationships.get(table, {}).items()
            if key != "metadata" and (key != "release_compatibility" or not available)
        }
        publication: dict[str, Any] = (
            _reply_pins(cursor, status["publication"], [table])[table]
            if table in status["publication"] else {"status": "unavailable"}
        )
        omitted = [] if detail else list(DESCRIBE_DETAIL)
        # What read_receipt_fields takes for this table, so a reader need not provoke a refusal to learn it. The
        # names are the same value in both replies; the meanings are a key only the detailed one carries.
        local = _connection_local_selection(cursor) if DATA_DIR is not None else None
        receipts = receipt_lookup.describe(table, entry, available=receipt_lookup.selected_member(
            _connection_index(cursor), local, R2_BASE_URL, table, _connection_rulemaking(cursor)) is not None)
        if receipts is not None and not detail:
            omitted.append(RECEIPT_FIELD_MEANINGS)
        if not detail:
            omitted.extend(f"metadata.{key}" for key in DESCRIBE_METADATA_DETAIL if key in entry)
        # Full release evidence sits once: in publication for an available view, else in relationship.
        held = publication if "release_compatibility" in publication else relationship
        if not detail and "release_compatibility" in held:
            held["release_compatibility"] = release_summary(held["release_compatibility"])
            where = "publication" if held is publication else "relationship"
            omitted += [f"{where}.release_compatibility.{item}" for item in RELEASE_INVENTORIES]
        return {
            "table": table,
            **_source_details(cursor),
            "available": available,
            "detail": {"full": detail, "omitted": omitted},
            **({"relationship": relationship} if table in relationships else {}),
            "publication": publication,
            "qualification": scope if qualified is None else {**scope, **qualified[table]},
            "joins": _table_joins(table, measurements=detail),
            **({"receipt_fields": receipts[0]} if receipts is not None else {}),
            **({RECEIPT_FIELD_MEANINGS: receipts[1]} if receipts is not None and detail else {}),
            "metadata": {key: [item["dataset"] for item in value] if table == "etl_receipts" and key == "datasets" else value
                         for key, value in entry.items()
                         if key not in {"table", "columns", "column_descriptions", "column_lineage",
                                        "legacy_column_descriptions"}
                         and (detail or key not in DESCRIBE_METADATA_DETAIL)},
            "metadata_basis": "Dictionary declarations and dated coverage notes; not live population measurements.",
            "schema_matches_declared": not any(differences.values()) if differences is not None else None,
            "schema_differences": differences,
            **({"schema_basis": "selected_publication"} if published_columns is not None else {}),
            "columns": [
                {"column_name": name, "column_type": dtype,
                 "description": legacy_meanings.get(name) or declared.get(name, {}).get("description")}
                for name, dtype in actual.items()
            ] if available else entry["columns"],
        }

    @tool
    def query_sql(
        sql: str,
        max_rows: Annotated[int, Field(ge=1, le=500)] = 25,
        max_cell_chars: Annotated[int | None, Field(ge=1)] = None,
    ) -> dict[str, Any]:
        """Run read-only SQL on Spicy Regs tables, returning up to max_rows rows.

        Only SELECT runs; DESCRIBE, SHOW, SUMMARIZE, VALUES, PRAGMA's table
        forms and FROM-first shorthand count as SELECT. Writes (COPY TO, ATTACH,
        CREATE, INSERT, DROP, EXPORT, SET, ...) and EXPLAIN (its ANALYZE form can
        write) are refused. Query tables listed by list_sources.
        Internal _spicy_ relations, dynamic SQL and storage statistics are refused.
        LIMIT exploratory queries. rows are arrays in the order of columns.
        truncated says whether rows beyond max_rows were cut from what the
        statement returned, not rows your LIMIT excluded: to learn whether more
        exist, COUNT. A reply past the reply limit is refused, saying how many
        rows fit. max_cell_chars cuts each longer text, list or struct cell (a
        list or struct as compact JSON) and lists each cut in truncated_cells
        with its full length. To page, ORDER BY a key with LIMIT n OFFSET m. SQL
        is DuckDB's dialect: `~` matches the whole string (regexp_matches for a
        substring, lower() for case). Alias shared column names in joins. sql
        echoes the statement. publication gives each named table its live data
        version, pinned rows, published_at, coverage kind (a window or sample is
        not the full history) and inputs: recorded parents. No recorded parents
        does not mean no inputs; reads bypassing the download helper are not
        recorded, as describe_table explains. Qualified-view pins keep
        the registered meaning and purpose limits even for SELECT value only;
        release compatible is not financial eligibility or current/net-money
        qualification. Call describe_table for full release evidence. Compare
        receipt, SQL, input and evidence pins after refresh; a later description
        may name a different release.
        """
        cursor = _get_connection().cursor()
        write_statement = _first_write_statement(cursor, sql)
        if write_statement is not None:
            raise ValueError(f"query_sql is read-only; refusing {write_statement} statement")

        with _statement_timeout(cursor):
            relationships = _connection_relationships(cursor)
            named = _tables_named(cursor, sql)
            internal = sorted(name for name in named if name.startswith("_spicy_"))
            if internal:
                raise ValueError("Internal relations are not queryable: " + ", ".join(internal))
            for name in named:
                release = relationships.get(name, {}).get("release_compatibility")
                if release is not None and release["status"] != "compatible":
                    raise ValueError(f"Qualified FEC view {name} is disabled: {relationships[name]['reason']}")
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            duplicates = [name for name, count in Counter(columns).items() if count > 1]
            if duplicates:
                raise ValueError(f"Duplicate result column names: {duplicates}; use AS aliases to give each a unique name")
            rows = cursor.fetchmany(max_rows + 1)
            publication = _publication_status(cursor)["publication"]
            named = _tables_named(cursor, sql)
            pins = _reply_pins(cursor, publication, [name for name in publication if name in named])
        # Each row is an array in the order of columns, which the reply states once: names repeated on every row
        # were 32% of a 399-row reply (round 6, ulrike's call 12).
        result_rows = [[_jsonify(value) for value in row] for row in rows[:max_rows]]
        truncated_cells = _bounded_cells(result_rows, columns, max_cell_chars)
        reply = {
            "sql": sql,
            **_source_details(cursor),
            "columns": columns,
            "row_count_shown": len(result_rows),
            "max_rows": max_rows,
            "truncated": len(rows) > max_rows,
            "max_cell_chars": max_cell_chars,
            "truncated_cells": truncated_cells,
            "rows": result_rows,
            "publication": _query_reply_pins(pins, relationships),
        }
        _refuse_oversized_rows(reply)
        return reply

    @tool
    def resolve_document_citations(
        # The description lists each kind with its table; an enum would list the kinds a second time, and a client
        # cuts a tool's description where it and the input schema together pass 2,048 characters.
        document_kind: Annotated[str, Field(description=DOCUMENT_KIND_TABLES)],
        document_key: str,
        # The maximum is a schema hint, as document_kind's enum is: a larger page is refused below with how to page.
        max_occurrences: Annotated[int, Field(ge=1, json_schema_extra={"maximum": MAX_OCCURRENCES})] = DEFAULT_OCCURRENCES,
        cite_kind: str | None = None,
        offset: Annotated[int, Field(ge=0)] = 0,
    ) -> dict[str, Any]:
        """Resolve a bounded document's held citations against this connection's selected targets.

        Findings keep spelling, text digest and rule; lookup checks neither
        precision nor legal effect. target_status_meaning defines each
        target_status the reply uses. acquisition_queue plans missing targets;
        nothing is acquired. document_kind is case-insensitive; its schema
        names each kind's table, whose key document_key takes
        (govinfo_package covers only house_activity_reports). document_key is exact and
        case-sensitive; a composite key is a compact JSON list in key order. A
        key nothing holds is refused. source_read.status read_none_found: read,
        none found; not_read: no read record (a held field's is in
        document_citation_reads); not_held: no longer held.
        Rows run in cite_kind order, then text position; cite_kind selects a
        kind, offset pages, and coverage.cite_kind_counts counts every kind. A
        page past the reply limit is refused with how many occurrences fit.
        occurrence_fields and acquisition_queue.shared_fields explain fields
        stated once. coverage.partial: rows left out of
        this page, an occurrence not looked up (reason_counts) or an unread
        document; never whole-document coverage.
        """
        from spicy_regs.acquisition_queue import build_missing_target_queue
        from spicy_regs.citation_resolution import CITE_KINDS, resolve_citations
        from spicy_regs.citation_sources import TEXT_SOURCES

        document_kind = _document_kind(document_kind)
        if max_occurrences > MAX_OCCURRENCES:
            raise ValueError(f"max_occurrences is at most {MAX_OCCURRENCES}; read a long document a page at a time "
                             "with offset (and cite_kind for one kind). coverage.cite_kind_counts states every "
                             "kind's rows.")
        connection = _get_connection()
        cursor = connection.cursor()
        with _statement_timeout(cursor):
            status = _publication_status(cursor)
            if "document_citations" not in status["tables"]:
                raise ValueError("document_citations is not available in this connection")
        from spicy_regs.citation_receipts import ensure_citation_inputs, selected_citation_inputs
        local = _connection_local_selection(cursor)
        # Restoration previously ran during connection construction, outside
        # the SQL statement timer. Keep that boundary and check local member
        # signatures in the statement contexts on both sides of preparation.
        with _diagnostic_phase("citation_preparation"):
            ensure_citation_inputs(
                connection, cursor, _connection_index(cursor), R2_BASE_URL, status["tables"],
                local_directory=local["directory"] if local else DATA_DIR,
                local_selection=local, memory_limit=MEMORY_LIMIT, temp_directory=TEMP_DIR,
            )
        with _statement_timeout(cursor):
            processing = selected_citation_inputs(cursor)
            citations = processing["tables"]["document_citations"]
            document = [document_kind, document_key]
            kind_counts = dict(cursor.execute(
                f'SELECT cite_kind, count(*) FROM "{citations}" WHERE document_kind = ? AND document_key = ? '
                "GROUP BY cite_kind ORDER BY cite_kind", document,
            ).fetchall())
            parent, held_field = SOURCE_TABLES[document_kind], document_kind in TEXT_SOURCES
            source_read, digest = _source_read(cursor, status["tables"], document_kind, document_key,
                                               cited=bool(kind_counts))
            if source_read["status"] == "not_held" and not kind_counts:
                raise ValueError(_unheld_document(cursor, document_kind, document_key, parent))
            if cite_kind is not None:
                requested, cite_kind = cite_kind, cite_kind.lower()
                if cite_kind not in CITE_KINDS:
                    raise ValueError(f"cite_kind {requested!r} is not a citation kind; the kinds are "
                                     f"{', '.join(CITE_KINDS)}. This document holds: {', '.join(kind_counts)}.")
            # span_start is stored as text: CAST orders it as the offset it is and refuses one that is not.
            # target_key separates the rows one range citation writes at one span, and text_sha256 (identity,
            # absent from a legacy file) a re-read text's rows, so the order is total and a page boundary stable.
            columns = [column[0] for column in cursor.execute(f'SELECT * FROM "{citations}" LIMIT 0').description]
            cursor.execute(
                f'SELECT * FROM "{citations}" WHERE document_kind = ? AND document_key = ?'
                + (" AND cite_kind = ?" if cite_kind is not None else "")
                + " ORDER BY cite_kind, CAST(span_start AS BIGINT), target_key, rule_version"
                + (", text_sha256" if "text_sha256" in columns else "") + " LIMIT ? OFFSET ?",
                [*document, *([cite_kind] if cite_kind is not None else []), max_occurrences + 1, offset],
            )
            rows = cursor.fetchall()
            # This tool reads text findings. A legacy file without the digest
            # column must not fall through the resolver's native-field API.
            occurrences = [
                {"text_sha256": None, **dict(zip(columns, row, strict=True))}
                for row in rows[:max_occurrences]
            ]
            result = resolve_citations(
                cursor, occurrences, status["publication"],
                source_digests={(document_kind, document_key): digest} if digest else {},
                max_target_keys=max_occurrences,
            )
            capped = len(rows) > max_occurrences
            result["coverage"]["cite_kind_counts"] = kind_counts
            result["coverage"]["occurrence_selection"] = {
                "status": "capped" if capped else "last_page" if offset else "complete_held_selection",
                "cite_kind": cite_kind, "offset": offset, "max_occurrences": max_occurrences,
                "meaning": "Held citation rows for this document; not extraction recall or source completeness.",
            }
            if capped or offset or source_read["status"] == "not_read":
                result["coverage"]["partial"] = True
        # Each resolved target carries its own target_snapshot; publication names the tables the lookup read.
        pins = _reply_pins(cursor, status["publication"], [
            name for name in ("document_citations", parent, *([HELD_FIELD_READS] if held_field else ()))
            if name in status["publication"]
        ])
        for name in ("document_citations", parent, *([HELD_FIELD_READS] if held_field else ())):
            if name in processing["publication"]:
                pins.setdefault(name, processing["publication"][name])
        source_pin = status["publication"].get(parent, {})

        def page(resolved: list[dict[str, Any]]) -> dict[str, Any]:
            """The reply for ``resolved``: this page's occurrences, or the first of them when sizing a refusal."""
            selected = {**result, "occurrences": resolved}
            reply = _jsonify(selected)
            occurrences, fields = _compact_occurrences(reply.pop("occurrences"))
            return {
                **_source_details(cursor), "occurrences": occurrences, "occurrence_fields": fields,
                "target_status_meaning": {status: TARGET_STATUS_MEANINGS[status]
                                          for status in sorted(reply["coverage"]["target_status_counts"])}, **reply,
                "document_kind": document_kind, "document_key": document_key,
                "max_occurrences": max_occurrences, "truncated": capped,
                "source_read": source_read,
                "acquisition_queue": _compact_queue(build_missing_target_queue(
                    selected, input_snapshots={document_kind: source_pin},
                    intended_query="Resolve the cited target for this held document", max_items=max_occurrences,
                ), source_pin),
                "publication": pins,
            }

        reply = page(result["occurrences"])
        _refuse_oversized_page(reply, page, result["occurrences"], offset)
        return reply

    @tool
    def read_receipt_fields(
        table: str,
        # The maximum is a schema hint, as max_occurrences' is: a longer list is refused in the tool's own words.
        keys: Annotated[list[dict[str, Any]],
                        Field(min_length=1, json_schema_extra={"maxItems": receipt_lookup.MAX_KEYS})],
        fields: Annotated[list[str], Field(min_length=1)],
    ) -> dict[str, Any]:
        """Read fields a table keeps in its ETL receipts, for up to 100 rows by key.

        Use this instead of query_sql for a value a table's notes say is kept
        in the receipt. describe_table's receipt_fields gives a table's
        identity fields and the fields its receipts carry, which differ by
        table. Each key is an object giving every identity field its exact
        value: text is case-sensitive and stays quoted even when it looks
        numeric; a whole-number field takes a number or its plain digits as
        text; a UTC timestamp field takes ISO text with an offset. An unknown table or field, a
        key that does not fit the identity and more than 100 keys are refused,
        saying what is valid. Values come back decoded. keys answers in request
        order; receipt is found, ambiguous (several accepted receipts, none
        chosen), receipt_missing (the table holds the key, no receipt does) or
        not_in_table. Each field of a found receipt has a state: stated,
        stated_empty, or for a field with no value unread or not_stated where
        the table declares a read marker for it, else null_unmarked (nothing
        says whether it was read). receipt_meaning and state_meaning define the
        words a reply uses; fields gives each field's meaning. receipts names
        the family or snapshot, receipt member, generation and policy version read. A lookup reads the
        admitted receipt key index when the selected publication supplies one.
        Without an index it reads the record id of every receipt of the table,
        so a table with more than 2,000,000 receipts is refused. Missing keys
        still require a bounded subject-table search. A reply past the reply limit is refused
        with how many keys fit.
        """
        cursor = _get_connection().cursor()
        with _statement_timeout(cursor):
            reply = receipt_lookup.read_fields(
                cursor, table=table, keys=keys, fields=fields, index=_connection_index(cursor),
                local=_connection_local_selection(cursor) if DATA_DIR is not None else None, base_url=R2_BASE_URL,
                entry=_table_metadata().get(table, {}), plain=_jsonify,
                snapshot=_connection_rulemaking(cursor),
            )
        reply = {**_source_details(cursor), **reply}
        receipt_lookup.refuse_oversized(reply, REPLY_CHARS, lambda value: len(_reply_text(value)[1]))
        return reply

    async def connection_status() -> CallToolResult:
        """Read bounded active/recent phase timings without opening a connection or using a tool slot.

        A phase marks where an operation is waiting; elapsed time does not establish progress or readiness.
        This observation neither starts a build nor cancels or retries a tool. It reads no source rows.
        """
        structured, text = _reply_text(_diagnostic_snapshot())
        return CallToolResult(content=[TextContent(type="text", text=text)], structured_content=structured)

    tools.append(_StrictTool.strict(connection_status, inspect.cleandoc(connection_status.__doc__ or "")))
    return tools


def build_server() -> MCPServer:
    """Build the MCP server with discovery, read-only queries, citation and receipt lookup; stdio, or HTTP via build_app."""
    return MCPServer("spicy-regs", instructions=INSTRUCTIONS, icons=ICONS, tools=_tools())


STATIC_DIR = Path(__file__).parent / "static"


@lru_cache(maxsize=1)
def _landing_page() -> bytes:
    """The setup page served at /, with its view list rendered from TABLES.

    The list used to be hand-maintained in the HTML and had drifted seven
    tables behind by the time this page was restored; substituting it here
    keeps the page honest as TABLES grows.
    """
    views = " ·\n        ".join(f'<code class="inline">{table}</code>' for table in TABLES)
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return (
        html.replace("<!--VIEWS-->", views)
        .replace("{{ MCP_URL }}", service_url("mcp") + "/mcp")
        .replace("{{ DOCS_URL }}", service_url("docs") + "/")
        .encode("utf-8")
    )


@lru_cache(maxsize=1)
def _landing_icon() -> bytes:
    """Decoded from ICON_DATA_URI so the PNG has exactly one home in the package."""
    return base64.b64decode(ICON_DATA_URI.split(",", 1)[1])


def _register_landing_page(mcp: MCPServer) -> None:
    """Serve the human-facing setup page alongside the MCP endpoint.

    Vercel served this as a static file at the site root; when that deploy was
    retired the page went with it, leaving mcp.spicy-regs.dev/ a bare 404. It
    lives in the canonical server now so every host (Cloud Run, the Cloudflare
    container) gets it without host-specific static-file config.
    """

    @mcp.custom_route("/", methods=["GET"])
    async def landing(_request: Request) -> Response:
        return Response(
            _landing_page(),
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "public, max-age=300"},
        )

    @mcp.custom_route("/icon.png", methods=["GET"])
    async def icon(_request: Request) -> Response:
        return Response(
            _landing_icon(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )


def build_app():
    """Build the stateless streamable-HTTP ASGI app (tools plus the landing page)."""
    mcp = build_server()
    _register_landing_page(mcp)
    return mcp.streamable_http_app(
        streamable_http_path="/mcp",
        stateless_http=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()
