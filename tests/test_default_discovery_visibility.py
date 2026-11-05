"""Default visibility changes neither table access nor the retained dictionary."""
import asyncio

import duckdb
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server

# Independently pinned owner scope: four intentional-empty tables and the five
# audited small retained families. Generic sampled coverage is not this policy.
HIDDEN = (
    "financial_changes", "section_classifications", "bill_summaries", "diff_summaries",
    "edis_documents", "edis_attachments", "edis_attachment_files",
    "sec_rulemakings", "sec_comment_files",
    "cftc_rule_observations", "cftc_comments", "cftc_comment_files",
    "inspectors_general_report_metadata", "supreme_court_index_observations",
    "supreme_court_opinion_observations", "supreme_court_pdf_observations",
)


def call(server, name, arguments):
    return asyncio.run(server.call_tool(name, arguments)).structured_content


@pytest.fixture
def local_reader(monkeypatch):
    connection = duckdb.connect(config={"threads": 1, "memory_limit": "64MB"})
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: connection)
    yield connection, mcp_server.build_server()
    connection.close()


@pytest.mark.parametrize("name", HIDDEN)
def test_hidden_default_table_remains_explicitly_described_and_queryable(local_reader, name):
    connection, server = local_reader
    connection.execute(f'CREATE TABLE "{name}" AS SELECT 7 AS marker')
    metadata = mcp_server._table_metadata()[name]
    default = call(server, "list_sources", {})
    assert default["subjects"] == []
    explicit = call(server, "list_sources", {"subject": metadata["subject"]})
    assert [entry["table"] for entry in explicit["subjects"][0]["tables"]] == [name]
    description = call(server, "describe_table", {"table": name})
    assert name in mcp_server.TABLES and description["available"] is True
    assert description["publication"]["coverage"] == metadata["kind"]
    result = call(server, "query_sql", {"sql": f'SELECT marker FROM "{name}"'})
    assert result["rows"] == [[7]]


def test_default_unavailable_preview_and_count_omit_only_owner_scope(local_reader):
    _, server = local_reader
    default = call(server, "list_sources", {})
    explicit = call(server, "list_sources", {"subject": "views"})
    assert not set(default["unavailable_tables"]) & set(HIDDEN)
    assert set(HIDDEN) <= set(explicit["unavailable_tables"])
    assert default["unavailable_table_count"] == len(explicit["unavailable_tables"]) - len(HIDDEN)
    for name in HIDDEN:
        description = call(server, "describe_table", {"table": name})
        assert description["available"] is False and description["columns"]
        assert description["metadata"]["summary"]


def test_small_empty_sampled_and_unknown_non_targets_stay_visible(local_reader, monkeypatch):
    connection, server = local_reader
    visible = ("dockets", "documents", "fec_source_records", "ferc_search_documents")
    metadata = {name: dict(value) for name, value in mcp_server._table_metadata().items()}
    metadata["dockets"]["kind"] = "unknown"
    monkeypatch.setattr(mcp_server, "_table_metadata", lambda: metadata)
    for name in visible:
        connection.execute(f'CREATE TABLE "{name}" (marker INTEGER)')
    default = call(server, "list_sources", {})
    names = {name for group in default["subjects"] for name in group["tables"]}
    assert names == set(visible)
    with pytest.raises(ToolError, match="Unknown subject"):
        call(server, "list_sources", {"subject": "not_a_subject"})
