"""Citation tools consume exact selected receipts while SQL sees native subjects."""
import hashlib

import duckdb
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server
from spicy_regs.etl_receipts import ReceiptContext, write_dataset
from spicy_regs.legislative_receipts import mapped_record, policy
from spicy_regs.selected_generations import SelectedDataset, remember_selection
from tests.test_legislative_receipts import row
from tests.test_mcp_server import _tool_data


def select(root, dataset, rows):
    context = ReceiptContext("citation-generation", "citation-attempt", "citation-test", [
        {"source_id": dataset, "sha256": "a" * 64}])
    subject, receipts = write_dataset(
        [(mapped_record(dataset, source), context) for source in rows], root / dataset, policy(dataset))
    remember_selection(root, [SelectedDataset(dataset, (subject,) if subject is not None else (),
                                               receipts, context.generation_id)])
    return subject, receipts


def connection(root, monkeypatch):
    monkeypatch.setattr(mcp_server, "DATA_DIR", root)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    return con, mcp_server.build_server()


def occurrence(digest, **overrides):
    return row("document_citations", document_kind="budget_volume", document_key="BUDGET-2027",
               cite_kind="public_law", target_key="119-public-1", span_start="0", span_end="12",
               matched_text="Public Law 1", text_sha256=digest, target_resolved="false",
               target_rule="public_law", rule_version="test-rule") | overrides


def test_current_print_citations_select_latest_parent_before_joining_shared_body(tmp_path, monkeypatch):
    from spicy_regs.govinfo_revisions import latest_package_rows

    digest = "sha256:" + "c" * 64
    select(tmp_path, "document_citations", [occurrence(digest, target_resolved="true")])
    parent = row("budget_volumes", package_id="BUDGET-2027", text_sha256=digest,
                 pages_read="1", rule_set_version="test", citation_rows="1")
    select(tmp_path, "budget_volumes", [
        parent | {"last_modified": "2026-09-01T00:00:00Z"},
        parent | {"last_modified": "2026-08-01T00:00:00Z"},
        parent | {"last_modified": "2026-07-01T00:00:00Z", "text_sha256": "sha256:" + "a" * 64},
    ])
    select(tmp_path, "laws", [row("laws", congress="119", law_type="public", number="1", law_id="119-public-1")])
    with connection(tmp_path, monkeypatch)[0] as con:
        result = _tool_data(mcp_server.build_server(), "resolve_document_citations", {
            "document_kind": "budget_volume", "document_key": "BUDGET-2027"})
        assert result["source_read"]["status"] == "read"
        assert len(result["occurrences"]) == 1
        assert result["occurrences"][0]["target_status"] == "found"
        current = latest_package_rows("budget_volumes")
        # Native body IDs keep their existing identity; one latest metadata
        # parent is selected before the current-document join.
        assert con.sql(f"SELECT count(*) FROM document_citations c JOIN ({current}) p "
                       "ON c.document_key = p.package_id AND c.body_version_id = p.body_version_id").fetchone()[0] == 1


@pytest.mark.parametrize("settled", [False, True])
def test_native_citation_tool_preserves_unsettled_key_and_private_processing(tmp_path, monkeypatch, settled):
    digest = "sha256:" + "c" * 64
    subjects, _ = select(tmp_path, "document_citations", [occurrence(digest, target_resolved=str(settled).lower())])
    select(tmp_path, "budget_volumes", [row("budget_volumes", package_id="BUDGET-2027",
                                          text_sha256=digest, pages_read="1", rule_set_version="test",
                                          citation_rows="1")])
    select(tmp_path, "laws", [row("laws", congress="119", law_type="public", number="1", law_id="119-public-1")])
    # Whether the key is settled is published with the row; the rule's version stays in the receipt.
    assert pq.read_table(subjects)["target_resolved"].to_pylist() == [settled]
    assert "rule_version" not in pq.read_schema(subjects).names
    with connection(tmp_path, monkeypatch)[0] as con:
        server = mcp_server.build_server()
        result = _tool_data(server, "resolve_document_citations", {
            "document_kind": "budget_volume", "document_key": "BUDGET-2027"})
        assert result["source_read"]["status"] == "read"
        assert result["occurrences"][0]["target_status"] == ("found" if settled else "not_checked")
        assert result["occurrences"][0]["reason"] == (None if settled else "unsettled_key")
        assert result["coverage"]["distinct_target_keys_read"] == int(settled)
        described = {r[0]: r[1] for r in con.execute("DESCRIBE document_citations").fetchall()}
        assert described["target_resolved"] == "BOOLEAN" and "rule_version" not in described
        assert result["occurrences"][0]["target_resolved"] == str(settled).lower()
        with pytest.raises(ToolError, match="Internal"):
            _tool_data(server, "query_sql", {"sql": "SELECT * FROM _spicy_citation_processing_document_citations"})
        with pytest.raises(ToolError, match="Internal"):
            _tool_data(server, "query_sql", {"sql": "SELECT * FROM _spicy_citation_inputs"})


def test_returned_citation_columns_are_public_and_the_tool_still_states_them_as_written(tmp_path, monkeypatch):
    """Owner decision, 2026-10-05: target_resolved and target_rule are subject columns again.

    Public SQL reads them typed from the subject. The tool reads the same row's whole original record, which the
    receipt still holds and the subject must agree with, so its reply keeps the writer's spelling and states each once.
    """
    digest = "sha256:" + "c" * 64
    written = [occurrence(digest, target_resolved="true"),
               occurrence(digest, span_start="20", span_end="32", target_rule="public_law:unstated")]
    subjects, _ = select(tmp_path, "document_citations", written)
    select(tmp_path, "budget_volumes", [row("budget_volumes", package_id="BUDGET-2027", text_sha256=digest,
                                          pages_read="1", rule_set_version="test", citation_rows="2")])
    select(tmp_path, "laws", [row("laws", congress="119", law_type="public", number="1", law_id="119-public-1")])
    assert pq.read_schema(subjects).equals(policy("document_citations").subject_schema)
    with connection(tmp_path, monkeypatch)[0]:
        server = mcp_server.build_server()
        public = _tool_data(server, "query_sql", {
            "sql": "SELECT span_start, target_resolved, typeof(target_resolved), target_rule, typeof(target_rule) "
                   "FROM document_citations ORDER BY span_start"})
        assert public["rows"] == [[0, True, "BOOLEAN", "public_law", "VARCHAR"],
                                  [20, False, "BOOLEAN", "public_law:unstated", "VARCHAR"]]
        reply = _tool_data(server, "resolve_document_citations", {
            "document_kind": "budget_volume", "document_key": "BUDGET-2027"})
        settled, unsettled = reply["occurrences"]
        assert (settled["target_resolved"], settled["target_status"]) == ("true", "found")
        assert (unsettled["target_resolved"], unsettled["reason"]) == ("false", "unsettled_key")
        # A rule that only restates the kind is said once for the reply; one that says more stays on its occurrence.
        assert reply["occurrence_fields"]["same_as"]["target_rule"] == "cite_kind"
        assert "target_rule" not in settled and unsettled["target_rule"] == "public_law:unstated"


def test_a_published_flag_its_receipt_contradicts_is_refused_before_the_tool_can_read_it(tmp_path, monkeypatch):
    """The subject and the receipt both hold target_resolved; the tool serves neither where they disagree."""
    source = occurrence("sha256:" + "c" * 64)
    context = ReceiptContext("citation-generation", "citation-attempt", "citation-test", [
        {"source_id": "document_citations", "sha256": "a" * 64}])
    contradicted = mapped_record("document_citations", source) | {"target_resolved": True}
    subject, receipts = write_dataset([(contradicted, context)], tmp_path / "document_citations",
                                      policy("document_citations"))
    assert subject is not None
    remember_selection(tmp_path, [SelectedDataset("document_citations", (subject,), receipts, context.generation_id)])
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    with connection(tmp_path, monkeypatch)[0]:
        server = mcp_server.build_server()
        _tool_data(server, "list_sources", {})
        with pytest.raises(ToolError, match="differs from its selected native subject"):
            _tool_data(server, "resolve_document_citations", {
                "document_kind": "budget_volume", "document_key": "BUDGET-2027"})


def test_held_text_zero_result_comes_from_receipt_only_read(tmp_path, monkeypatch):
    text = "No cited legal text."
    digest = "sha256:" + hashlib.sha256(text.encode()).hexdigest()
    select(tmp_path, "document_citations", [])
    select(tmp_path, "report_sections", [row("report_sections", package_id="CRPT-119hrpt1",
        part_id="1", seq="0", body=text)])
    key = '["CRPT-119hrpt1","1","2026-09-18T12:00:00Z","0"]'
    select(tmp_path, "document_citation_reads", [row("document_citation_reads",
        document_kind="report_section", document_key=key, text_sha256=digest,
        citation_rows="0", read_at="2026-10-04T00:00:00Z", rule_set_version="test")])
    with connection(tmp_path, monkeypatch)[0] as con:
        server = mcp_server.build_server()
        reply = _tool_data(server, "resolve_document_citations", {
            "document_kind": "report_section", "document_key": key})
        assert reply["source_read"]["status"] == "read_none_found"
        # The private cache must keep only identities and hashes, never a second
        # copy of a source body (comments and full document text can be large).
        cache = "_spicy_citation_digests_report_sections"
        assert {name for name, *_ in con.execute(f"DESCRIBE {cache}").fetchall()} == {
            "package_id", "part_id", "last_modified", "seq", "sha256_body"}
        assert con.execute(f"SELECT sha256_body FROM {cache}").fetchone() == (digest,)
        assert reply["occurrences"] == []
        assert "document_citation_reads" not in mcp_server._available_tables(con)
        assert reply["publication"]["document_citation_reads"]["generation_id"] == "citation-generation"


def test_receiptless_citation_table_is_not_a_tool_fallback(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE document_citations(document_kind VARCHAR)")
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        with pytest.raises(ToolError, match="requires selected native ETL receipts"):
            _tool_data(server, "resolve_document_citations", {
                "document_kind": "budget_volume", "document_key": "BUDGET-2027"})


@pytest.mark.parametrize("blocked", ["lock", "private"])
def test_lazy_citation_preparation_obeys_request_cancellation(monkeypatch, blocked):
    import threading
    from time import monotonic, sleep
    from spicy_regs import citation_receipts
    from spicy_regs.runtime_bounds import RequestBound, checkpoint

    with duckdb.connect(config={"threads": 1}) as connection:
        cursor = connection.cursor()
        held = threading.Lock()
        private_connections = []
        if blocked == "lock":
            held.acquire()
            citation_receipts._initialization_locks[connection] = held
        def blocked_admission(private, adapter, available):
            private_connections.append(private)
            assert private in bound._targets and cursor in bound._targets
            while True:
                sleep(.005)
                checkpoint()
        monkeypatch.setattr(citation_receipts, "install_citation_inputs", blocked_admission)
        bound = RequestBound(.1, "citation preparation cancelled")
        started = monotonic()
        try:
            with pytest.raises(TimeoutError, match="citation preparation cancelled"):
                with bound.scope():
                    citation_receipts.ensure_citation_inputs(connection, cursor, {}, "", {})
            assert monotonic() - started < 1
            assert not bound._targets
            if blocked == "lock":
                assert held.locked() and not private_connections
            else:
                assert len(private_connections) == 1
                with pytest.raises(duckdb.ConnectionException):
                    private_connections[0].execute("SELECT 1")
        finally:
            if blocked == "lock":
                held.release()
            cursor.close()
