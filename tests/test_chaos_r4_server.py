"""Round-4 chaos repairs at the server boundary: registered descriptions, kind refusals, release summaries, export rows.

Evidence: corpora/mcp-chaos-2026-10-02/round4/phase2-server.md (S1-S4, S10, the double send) and phase3-review.md.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from copy import deepcopy
from typing import Any

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError
from starlette.testclient import TestClient

from spicy_regs import fec_release as release, mcp_server as server
from spicy_regs.sources import publication as pub
from tests.test_chaos_r3_server import _fec_specs
from tests.test_generation_mcp import connection_fixture, serve_documents
from tests.test_mcp_fec_release import configure, connection
from tests.test_mcp_query_results import call
from tests.citation_fixtures import prepare_citation_inputs
from tests.test_mcp_relationships import citation_connection
from tests.test_mcp_server import _listed, _tool_data

# The client's cap is 2,048 characters (Claude Code's MAX_MCP_DESCRIPTION_LENGTH default), and it counts the
# compact input schema with the description: round 5 saw query_sql's 1,728 + 322 = 2,050 cut at character 1,711
# for three personas while describe_table's 1,832 + 190 = 2,022 was not (round5/audit-oyelaran.md, finding 5).
CLIENT_BUDGET = 2_048
#: Room every tool keeps under the cap (round 6 review, item 2): rounds 5 and 6 ended 4 to 16 characters under it, so
#: one more word or a schema change in a pydantic release would cut a description mid-sentence.
HEADROOM = 40


# S3 and the double send: what the client is sent.

def test_every_registered_description_fits_the_client_cap_without_indentation():
    tools = asyncio.run(server.build_server().list_tools())
    # Named, so a tool that failed to register cannot pass by not being measured.
    assert [tool.name for tool in tools] == ["lookup_agency", "list_sources", "describe_table", "query_sql",
                                             "resolve_document_citations", "connection_status"]
    for tool in tools:
        description = tool.description or ""
        sent = len(description) + len(json.dumps(tool.input_schema, separators=(",", ":")))
        assert sent <= CLIENT_BUDGET - HEADROOM, (tool.name, len(description), sent)
        assert not any(line[:1].isspace() for line in description.splitlines()), tool.name


def test_registered_descriptions_are_the_cleaned_docstrings():
    tools = {tool.name: tool.description or "" for tool in asyncio.run(server.build_server().list_tools())}
    assert tools["query_sql"].startswith("Run read-only SQL")
    assert tools["query_sql"] == inspect.cleandoc(tools["query_sql"])


def test_text_content_is_the_compact_json_of_the_structured_content(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE comments_index AS SELECT 'FDA' AS agency_code, 1.5 AS row_count")
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with TestClient(server.build_app()) as client:
            for name, arguments in (("query_sql", {"sql": "SELECT 1 AS one, 'é' AS accent, 0.1 AS tenth"}),
                                    ("describe_table", {"table": "comments_index"}), ("list_sources", {})):
                result = call(client, name, arguments)
                [content] = result["content"]
                assert content["text"] == json.dumps(result["structuredContent"], separators=(",", ":"),
                                                     ensure_ascii=False)


# S1: resolve_document_citations refuses a kind no writer emits.

@pytest.mark.parametrize(("kind", "closest"), [
    ("federal_register", None),
    ("court_opinion", "court_opinion_derived_pdf"),
    ("house_activity_reports", "govinfo_package"),
    ("house_activity_report", "govinfo_package"),
    ("budget_volumes", "budget_volume"),
])
def test_an_unsupported_kind_is_refused_naming_the_supported_kinds(monkeypatch, kind, closest):
    with citation_connection() as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with pytest.raises(ToolError) as refused:
            _tool_data(server.build_server(), "resolve_document_citations",
                       {"document_kind": kind, "document_key": "CRPT-example"})
    message = str(refused.value)
    assert repr(kind) in message and "govinfo_package" in message and "comment_inline" in message
    if closest is not None:
        assert f"closest supported kind is {closest!r}" in message


def test_kinds_compare_case_insensitively(monkeypatch):
    with citation_connection() as con:
        monkeypatch.setattr(server, "_get_connection", lambda: prepare_citation_inputs(con))
        result = _tool_data(server.build_server(), "resolve_document_citations",
                            {"document_kind": "Govinfo_Package", "document_key": "CRPT-example"})
    assert result["document_kind"] == "govinfo_package"
    assert len(result["occurrences"]) == 2 and result["source_read"]["status"] == "read"


# S4: describe_table names the release inventories it leaves out.

def test_describe_default_summarizes_release_evidence_and_names_what_it_omits(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        name = specs[0].view.name
        stored = server._connection_relationships(con)[name]["release_compatibility"]
        compact = _tool_data(mcp, "describe_table", {"table": name})
        summary = compact["publication"]["release_compatibility"]
        assert "acceptance_receipts" not in summary
        assert summary["acceptance_receipt_count"] == len(stored["acceptance_receipts"]) == 1
        assert summary["dependencies"] == {table: {"family": pin["family"], "generation": pin["generation"]}
                                           for table, pin in stored["dependencies"].items()}
        for key in ("status", "reasons", "receipt_sha256", "sql_sha256", "interpretation", "consumer",
                    "evidence_generations", "required_evidence_generations", "population", "as_of",
                    "image_identity_basis"):
            assert summary[key] == stored[key]
        assert compact["detail"]["omitted"] == [
            "joins[].measurement", "qualification.ledger_statements",
            "publication.release_compatibility.dependencies[].descriptor",
            "publication.release_compatibility.acceptance_receipts",
        ]
        full = _tool_data(mcp, "describe_table", {"table": name, "detail": True})
        assert full["publication"]["release_compatibility"] == stored and full["detail"]["omitted"] == []
        assert server._connection_relationships(con)[name]["release_compatibility"] == stored


def test_an_unavailable_views_description_summarizes_its_relationship_record(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", "sha256:" + "0" * 64)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        compact = _tool_data(server.build_server(), "describe_table", {"table": specs[0].view.name})
    assert not compact["available"]
    record = compact["relationship"]["release_compatibility"]
    assert record["status"] == "disabled" and record["reasons"] and "acceptance_receipts" not in record
    assert "relationship.release_compatibility.acceptance_receipts" in compact["detail"]["omitted"]


def test_query_replies_keep_their_release_pins(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        name = specs[0].view.name
        pins = _tool_data(server.build_server(), "query_sql", {"sql": f"SELECT * FROM {name}"})["publication"][name]
    assert set(pins["release_compatibility"]) == set(release.QUERY_RELEASE_FIELDS)


def test_release_summary_does_not_mutate_the_record():
    record = {"status": "compatible", "acceptance_receipts": ["sha256:" + "a" * 64],
              "dependencies": {"t": {"family": "f", "generation": "g", "descriptor": {"members": [1]}}, "u": None}}
    saved = deepcopy(record)
    summary = release.release_summary(record)
    assert summary["dependencies"] == {"t": {"family": "f", "generation": "g"}, "u": None}
    assert summary["acceptance_receipt_count"] == 1 and record == saved


# S2: the comments exports state the receipt's rows, labelled as an export, never as a pin.

RECEIPT: dict[str, Any] = {
    "format_version": 1,
    "source": {"schema_id": 6, "snapshot_id": 2757430127503624538, "table_uuid": "01a0d3fc-be18-70b0-abeb-e0f6e58d6cff"},
    "files": {
        "comments_index.parquet": {"bytes": 422071, "etag": '"7ca7cab42e209a8d0e859e534dec6766"', "rows": 143564,
                                   "sha256": "98d174b23c0c0ccd3df8d8d5575f9e108aaba532e20ee2ef927d028cca40cab0"},
        "comments.parquet": {"bytes": 4365019780, "etag": '"de9026c8aa6da8319674df98321ab35d-521"', "rows": 26408987,
                             "sha256": "8c3caee14ab43f9ef2acc5c8e49331a0d25cb6df3918ee860430a12bcd7159da"},
        "comments/agency/agency_code=ACF/part-0.parquet": {"bytes": 1, "etag": '"x"', "rows": 1, "sha256": "c" * 64},
    },
}


def _export_server(tmp_path, monkeypatch, *, receipt=RECEIPT, etag=None):
    """A remote build over one legacy comments_index file, its export receipt and a HEAD answering ``etag``."""
    legacy = tmp_path / "comments_index.parquet"
    pq.write_table(pa.table({"agency_code": ["FDA"], "row_count": [1]}), legacy)
    documents = {f"{server.R2_BASE_URL}/comments-publication.json": receipt} if receipt is not None else {}
    built = connection_fixture(monkeypatch, {"families": {}}, {"comments_index.parquet": legacy},
                               tables=("comments_index",), documents=documents)
    heads = []

    def head(url, **_):
        heads.append(url)
        record = RECEIPT["files"][url.rsplit("/", 1)[1]]
        headers = {"etag": etag or record["etag"], "content-length": str(record["bytes"])}
        return httpx.Response(200, headers=headers, request=httpx.Request("HEAD", url))

    monkeypatch.setattr(pub.httpx, "head", head)
    return built, heads


def _largest_integer(value: Any) -> int:
    """The largest integer magnitude anywhere in a JSON-shaped value."""
    if isinstance(value, dict):
        return max(map(_largest_integer, value.values()), default=0)
    if isinstance(value, list):
        return max(map(_largest_integer, value), default=0)
    return abs(value) if isinstance(value, int) and not isinstance(value, bool) else 0


def test_a_matching_export_receipt_states_its_rows_labelled_as_an_export(tmp_path, monkeypatch):
    built, heads = _export_server(tmp_path, monkeypatch)
    con = server._build_connection()
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    mcp = server.build_server()
    [entry] = [t for t in _listed(mcp) if t["table"] == "comments_index"]
    assert entry["rows"] == 143_564 and entry["rows_basis"] == "comments_export_receipt"
    assert heads == [f"{server.R2_BASE_URL}/comments_index.parquet"]  # only the served export, not every listed file
    pin = _tool_data(mcp, "describe_table", {"table": "comments_index"})["publication"]
    assert pin["status"] == "legacy_unversioned" and pin["rows_basis"] == "comments_export_receipt"
    assert pin["export_receipt"] == {
        "receipt_sha256": "sha256:" + pub.hashlib.sha256(json.dumps(RECEIPT).encode()).hexdigest(),
        "sha256": "sha256:" + RECEIPT["files"]["comments_index.parquet"]["sha256"],
        "etag": RECEIPT["files"]["comments_index.parquet"]["etag"], "bytes": 422071,
        "catalog_snapshot_id": "2757430127503624538",
    }
    # Round 5 (oyelaran, finding 6): a JavaScript client rounds an integer past 2**53, so the reply states none.
    assert _largest_integer(pin) < 2**53
    # Derived views embed the status pin; the receipt joins replies only.
    assert server._publication_status(con.cursor())["publication"]["comments_index"] == {"status": "legacy_unversioned"}
    built[0].inner.close()


def test_an_export_object_that_moved_since_its_receipt_states_no_rows(tmp_path, monkeypatch):
    built, _ = _export_server(tmp_path, monkeypatch, etag='"moved"')
    con = server._build_connection()
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    [entry] = [t for t in _listed(server.build_server())
               if t["table"] == "comments_index"]
    assert entry["rows"] is None and entry["rows_basis"] == "export_receipt_does_not_match_object"
    built[0].inner.close()


def test_without_a_receipt_an_export_states_no_rows_and_no_basis(tmp_path, monkeypatch):
    built, heads = _export_server(tmp_path, monkeypatch, receipt=None)
    con = server._build_connection()
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    [entry] = [t for t in _listed(server.build_server())
               if t["table"] == "comments_index"]
    assert entry == {"table": "comments_index", "label": entry["label"], "coverage": entry["coverage"], "rows": None}
    assert heads == []
    built[0].inner.close()


def test_a_moved_receipt_rebuilds_the_connection(tmp_path, monkeypatch):
    _export_server(tmp_path, monkeypatch)
    first = server._build_connection()
    assert server._refreshed(first) is first
    moved: dict[str, Any] = deepcopy(RECEIPT)
    moved["source"]["snapshot_id"] += 1
    serve_documents(monkeypatch, {f"{server.R2_BASE_URL}/comments-publication.json": moved})
    assert server._refreshed(first) is not first


# S10 class A: the summary and detailed-summary decision columns name what the rule does not do.

def test_summary_measure_reasons_name_the_layouts_that_never_state_the_field():
    from spicy_regs.fec_financial_rules import _SUMMARY_MAPPINGS

    specs = {spec.view.name: spec.view for spec in _fec_specs()}
    for field in ("NET_CONTB", "TTL_RECEIPTS", "TTL_DISB"):
        reason = specs[f"fec_reported_financial_summaries_{field.lower()}_decision"].column_descriptions["reason"]
        missing = [layout for layout, rule in _SUMMARY_MAPPINGS.items() if field not in rule.money_fields]
        assert "summary_measure_missing_ambiguous_or_inexact" in reason and all(layout in reason for layout in missing)
        assert "Total_Receipt" in reason
    present = [layout for layout, rule in _SUMMARY_MAPPINGS.items() if "TTL_RECEIPTS" in rule.money_fields]
    reason = specs["fec_reported_financial_summaries_ttl_receipts_decision"].column_descriptions["reason"]
    assert not any(layout in reason for layout in present)


def test_detailed_summary_values_say_outflows_keep_a_positive_sign():
    specs = {spec.view.name: spec.view for spec in _fec_specs()}
    for table in ("fec_receipts", "fec_intercommittee_transactions"):
        value = specs[f"{table}_detailed_summary_component_decision"].column_descriptions["value"]
        assert "22Y" in value and "positive" in value and "negative_sign_preserved_refund_not_inferred" in value
