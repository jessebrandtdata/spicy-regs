"""On-demand citation validation preserves locked serving and atomic publication."""
import gc
import json
import shutil
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest
import pyarrow.parquet as pq

from spicy_regs import citation_receipts as inputs, mcp_server
from tests.test_citation_receipts import select, occurrence
from tests.test_mcp_server import _listed, _tool_data
from tests.receipt_lookup_fixtures import index_of, published


def locked():
    con = duckdb.connect(config={"threads": 2})
    mcp_server._apply_security_settings(con)
    return con


def publish(private, adapter, available):
    name = inputs.PROCESSING_PREFIX + "document_citations"
    private.execute(f'CREATE TABLE "{name}"(value VARCHAR)')
    private.execute(f'INSERT INTO "{name}" VALUES (?), (?)', ["exact spelling", None])
    record = {"tables": {"document_citations": name}, "sources": {},
              "publication": {"document_citations": {"generation_id": "held"}}, "selected_sources": []}
    private.execute(f"CREATE TABLE {inputs.INPUT_RECORD}(snapshot VARCHAR)")
    private.execute(f"INSERT INTO {inputs.INPUT_RECORD} VALUES (?)", [json.dumps(record)])


def initialize(con):
    with con.cursor() as cursor:
        inputs.ensure_citation_inputs(con, cursor, {"families": {}}, "", ["document_citations"])


def test_locked_transfer_preserves_rows_pins_and_settings(monkeypatch, tmp_path):
    calls = []

    def checked(private, adapter, available):
        calls.append(private)
        publish(private, adapter, available)

    monkeypatch.setattr(inputs, "install_citation_inputs", checked)
    with locked() as con:
        initialize(con)
        initialize(con)
        assert len(calls) == 1
        assert con.execute(f'SELECT * FROM "{inputs.PROCESSING_PREFIX}document_citations"').fetchall() == [
            ("exact spelling",), (None,)]
        assert inputs.selected_citation_inputs(con)["publication"]["document_citations"] == {
            "generation_id": "held"}
        assert con.execute("SELECT current_setting('allowed_paths')").fetchone() == ([],)
        assert con.execute("SELECT current_setting('enable_external_access')").fetchone() == (False,)
        with pytest.raises(duckdb.Error, match="locked"):
            con.execute("SET enable_external_access=true")
        outside = tmp_path / "private.txt"
        outside.write_text("not selected")
        with pytest.raises(duckdb.Error, match="disabled"):
            con.execute("SELECT * FROM read_text(?)", [str(outside)])
    with pytest.raises(duckdb.Error, match="closed"):
        calls[0].execute("SELECT 1")


@pytest.mark.parametrize("failure", ["validation", "transfer"])
def test_failed_preparation_rolls_back_and_explicit_request_can_retry(monkeypatch, failure):
    expected = ValueError("exact validation refusal")
    original_copy = inputs._copy_inputs
    calls = []

    def prepare(private, adapter, available):
        calls.append(private)
        publish(private, adapter, available)
        if failure == "validation" and len(calls) == 1:
            raise expected

    def copy(source, destination, record):
        original_copy(source, destination, record)
        if len(calls) == 1:
            raise expected

    monkeypatch.setattr(inputs, "install_citation_inputs", prepare)
    if failure == "transfer":
        monkeypatch.setattr(inputs, "_copy_inputs", copy)
    with locked() as con:
        with pytest.raises(ValueError) as raised:
            initialize(con)
        assert raised.value is expected
        assert con.execute("SHOW TABLES").fetchall() == []
        initialize(con)
        assert inputs.selected_citation_inputs(con)["tables"]
        assert len(calls) == 2
    for private in calls:
        with pytest.raises(duckdb.Error, match="closed"):
            private.execute("SELECT 1")


def test_concurrent_calls_prepare_once_while_other_cursor_remains_usable(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    calls = []

    def prepare(private, adapter, available):
        calls.append(private)
        entered.set()
        assert release.wait(3)
        publish(private, adapter, available)

    monkeypatch.setattr(inputs, "install_citation_inputs", prepare)
    with locked() as con, ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(initialize, con)
        try:
            assert entered.wait(3)
            second = workers.submit(initialize, con)
            with con.cursor() as other:
                assert other.execute("SELECT 17").fetchone() == (17,)
                assert mcp_server._available_tables(other) == []
        finally:
            release.set()
        first.result(timeout=3)
        second.result(timeout=3)
        assert len(calls) == 1


def test_connections_initialize_independently_and_registry_does_not_retain_them(monkeypatch):
    calls = []

    def prepare(private, adapter, available):
        calls.append(available)
        publish(private, adapter, available)

    monkeypatch.setattr(inputs, "install_citation_inputs", prepare)
    first, replacement = locked(), locked()
    try:
        initialize(first)
        initialize(replacement)
        assert len(calls) == 2
        assert inputs.selected_citation_inputs(first) == inputs.selected_citation_inputs(replacement)
        held = weakref.ref(first)
    finally:
        first.close()
        replacement.close()
    del first
    gc.collect()
    assert held() is None


def test_real_source_listing_and_sql_do_not_prepare_citations(tmp_path, monkeypatch):
    select(tmp_path, "document_citations", [occurrence("sha256:" + "c" * 64)])
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())

    def forbidden(*args, **kwargs):
        pytest.fail("basic reader invoked citation restoration")

    monkeypatch.setattr(inputs, "install_citation_inputs", forbidden)
    with mcp_server._build_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        assert "document_citations" in {entry["table"] for entry in _listed(server)}
        assert not inputs._has_inputs(con)
        result = _tool_data(server, "query_sql", {"sql": "SELECT count(*) AS n FROM document_citations"})
        assert result["rows"] == [[1]]


def test_downloaded_receipts_never_enter_public_allowed_paths(tmp_path, monkeypatch):
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.sources import publication

    subject, receipts = select(tmp_path / "original", "document_citations", [occurrence("sha256:" + "c" * 64)])
    bucket = tmp_path / "bucket"
    entry = published(bucket, "citations", {"document_citations": subject}, pq.read_table(receipts),
                      generation="citation-generation")
    shared = bucket / entry["prefix"] / entry["etlReceipts"]["key"]
    index = index_of({"citations": entry})
    fetched = []

    def fetch(self, member, destination):
        # Synthetic held files stand in for downloads; retain the actual member pin checks.
        source = bucket / member.path
        identity = publication.file_identity(source)
        assert identity["sha256"] == member.sha256 and identity["bytes"] == member.byte_size
        shutil.copyfile(source, destination)
        fetched.append((member.key, destination))

    monkeypatch.setattr(ReceiptAdapter, "_fetch", fetch)
    with locked() as con, con.cursor() as cursor:
        inputs.ensure_citation_inputs(con, cursor, index, "https://unused.invalid", ["document_citations"])
        assert inputs.selected_citation_inputs(cursor)["publication"]["document_citations"]["receipt_sha256"] == (
            entry["etlReceipts"]["sha256"])
        assert any(name == "etl_receipts.parquet" for name, _ in fetched)
        assert all(not path.exists() for _, path in fetched)
        assert con.execute("SELECT current_setting('allowed_paths')").fetchone() == ([],)
        with pytest.raises(duckdb.Error, match="disabled"):
            con.execute("SELECT * FROM read_parquet(?)", [str(shared)])
