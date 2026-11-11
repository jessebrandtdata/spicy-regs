"""Discovery avoids row restoration; one request budget also bounds cold admission."""
import asyncio
from time import monotonic, sleep
from threading import Event, Timer

import pytest

from spicy_regs import etl_bulk, local_data, mcp_server as server, selected_generations
from spicy_regs.runtime_bounds import RequestBound, checkpoint, current_bound
from tests.test_local_native_selection import selected_family
from tests.test_mcp_server import _listed, _records, _tool_data

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


@pytest.fixture(autouse=True)
def reset_connections():
    server._reset_connection_cache()
    yield
    for con in (server._cached_connection, server._cached_discovery_connection):
        if con is not None:
            con.close()
    server._reset_connection_cache()


def test_relationship_declarations_do_not_touch_source_schemas():
    from spicy_regs.relationship_views import describe_relationship_views

    class NoDatabaseWork:
        def execute(self, *args, **kwargs):
            raise AssertionError('Discovery must not bind relationship SQL or read source schemas')

    pin = {'status': 'managed_generation', 'artifact_digest': 'sha256:' + 'a' * 64}
    registry = describe_relationship_views(NoDatabaseWork(), ['members'], {'members': pin})
    member_views = [item for item in registry.values() if item['dependencies'] == ['members']]
    assert member_views
    for entry in member_views:
        assert entry['status'] == 'deferred'
        assert entry['metadata']['query_admission'] == 'pending_relationship_binding'
        assert entry['metadata']['input_publications'] == {'members': pin}
        assert 'column_lineage' not in entry['metadata']
        assert entry['metadata']['availability_basis'] == 'Declared dependencies only; runtime schema binding and query admission remain pending.'
    assert registry['comment_document_references']['status'] == 'unavailable'


def test_general_discovery_defers_binding_but_query_preserves_schema_refusal(monkeypatch):
    import duckdb

    def connection(publication=None):
        con = duckdb.connect()
        con.execute('CREATE TABLE members (bioguide_id VARCHAR, fec_ids_json VARCHAR, observed_at VARCHAR, roster VARCHAR)')
        con.execute("INSERT INTO members VALUES ('X', '[\"S8WA00194\"]', 'today', 'current')")
        con.execute('CREATE TABLE comments (comment_id VARCHAR, docket_id VARCHAR)')
        server._install_relationship_views(con)
        server._apply_security_settings(con)
        return con

    monkeypatch.setattr(server, '_build_connection', connection)
    mcp = server.build_server()
    listed = _tool_data(mcp, 'list_sources', {'subject': 'views'})
    # Find the maintained name rather than assuming one from the source field.
    discovery_connection = server._cached_discovery_connection
    assert discovery_connection is not None
    registry = server._connection_relationships(discovery_connection)
    name = next(name for name, item in registry.items()
                if item['dependencies'] == ['members'] and name.endswith('_occurrences'))
    assert name in {name for group in listed['deferred_views'] for name in group['views']}
    described = _tool_data(mcp, 'describe_table', {'table': name})
    assert not described['available'] and described['columns'] == []
    assert described['metadata']['query_admission'] == 'pending_relationship_binding'
    assert server._cached_connection is None
    result = _tool_data(mcp, 'query_sql', {'sql': f'SELECT target_key FROM "{name}"'})
    assert _records(result) == [{'target_key': 'S8WA00194'}]
    with pytest.raises(Exception, match='comment_document_references'):
        _tool_data(mcp, 'query_sql', {'sql': 'SELECT * FROM comment_document_references'})
    query_connection = getattr(server, '_cached_connection')
    assert query_connection is not None
    query_registry = server._connection_relationships(query_connection)
    assert query_registry['comment_document_references']['status'] == 'unsupported'
    assert server._cached_connection is not server._cached_discovery_connection


def test_queries_bind_only_requested_relationships_and_expand_same_pins(monkeypatch):
    import duckdb

    builds = []
    selected = {'families': {}, 'test_generation': 'first'}

    def connection(publication=None):
        index = selected if publication is None else publication.index
        builds.append((index, server._requested_relations.get()))
        con = duckdb.connect()
        con.execute('CREATE TABLE _spicy_publication (snapshot VARCHAR)')
        import json
        con.execute('INSERT INTO _spicy_publication VALUES (?)', [json.dumps(index)])
        con.execute('CREATE TABLE members (bioguide_id VARCHAR, fec_ids_json VARCHAR, observed_at VARCHAR, roster VARCHAR)')
        con.execute("INSERT INTO members VALUES ('X', '[\"S8WA00194\"]', 'today', 'current')")
        con.execute('CREATE TABLE comments (comment_id VARCHAR, docket_id VARCHAR, comment_reference_values_json VARCHAR, '
                    'comment_on_document_id VARCHAR, comment_on_object_id VARCHAR, original_document_id VARCHAR)')
        server._install_relationship_views(con)
        server._apply_security_settings(con)
        return con

    monkeypatch.setattr(server, '_build_connection', connection)
    mcp = server.build_server()
    assert _records(_tool_data(mcp, 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})) == [{'bioguide_id': 'X'}]
    con = getattr(server, '_cached_connection')
    assert con is not None
    registry = server._connection_relationships(con)
    assert all(item['status'] != 'available' for item in registry.values())
    names = [name for name, item in registry.items()
             if item['dependencies'] == ['members'] and name.endswith('_pairs')]
    assert names
    first = con
    for name in names:
        _tool_data(mcp, 'query_sql', {'sql': f'SELECT * FROM "{name}"'})
    con = getattr(server, '_cached_connection')
    assert con is not None and con is not first
    assert builds[1][0] == selected
    assert all(item['status'] != 'available' for item in server._connection_relationships(con).values()
               if item['dependencies'] != ['members'])
    assert _records(_tool_data(mcp, 'query_sql', {'sql': 'SELECT * FROM comment_document_references'})) == []
    later = getattr(server, '_cached_connection')
    assert later is not None and later is not con
    assert server._connection_relationships(later)['comment_document_references']['status'] == 'available'
    assert builds[-1][0] == selected
    # A changed selected pointer takes the maintained refresh path, rather
    # than expanding a stale connection with new-generation source views.
    selected = {'families': {}, 'test_generation': 'second'}
    monkeypatch.setattr(server, '_read_publication', lambda: server._Publication(selected, None, None))
    monkeypatch.setattr(server, '_CONNECTION_TTL_SECONDS', 0)
    _tool_data(mcp, 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})
    assert builds[-1][0] == selected
    with pytest.raises(Exception, match='read-only'):
        _tool_data(mcp, 'query_sql', {'sql': 'CREATE TABLE bypass AS SELECT 1'})


@pytest.mark.parametrize('changed_record', ['_spicy_local_selection', '_spicy_fec_release'])
def test_relationship_expansion_refuses_changed_local_selection_or_release(monkeypatch, changed_record):
    import duckdb
    import json

    def connection(version):
        con = duckdb.connect()
        for name, record in {
            '_spicy_relationships': {'needed': {'status': 'deferred'}},
            '_spicy_local_selection': {'signatures': {}, 'generation': version if changed_record == '_spicy_local_selection' else 'held'},
            '_spicy_fec_release': {'receipt': version if changed_record == '_spicy_fec_release' else 'held'},
        }.items():
            con.execute(f'CREATE TABLE {name} (snapshot VARCHAR)')
            con.execute(f'INSERT INTO {name} VALUES (?)', [json.dumps(record)])
        return con

    with connection('first') as current:
        replacement = connection('second')
        monkeypatch.setattr(server, '_build_connection', lambda publication: replacement)
        token = server._requested_relations.set({'needed'})
        try:
            with pytest.raises(RuntimeError, match='publication or release changed'):
                server._expand_query_connection(current)
            assert current.execute('SELECT 1').fetchone() == (1,)
            with pytest.raises(duckdb.ConnectionException, match='closed'):
                replacement.execute('SELECT 1')
        finally:
            server._requested_relations.reset(token)


def test_financial_discovery_defers_exact_rows_but_query_still_admits(tmp_path, monkeypatch):
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from tests.test_mcp_fec_release import configure, connection
    specs, index, *_ = configure(tmp_path, monkeypatch)
    restored = []
    original = ReceiptAdapter.restore_originals

    def restoring(self, table, *args, **kwargs):
        restored.append(table)
        return original(self, table, *args, **kwargs)

    monkeypatch.setattr(ReceiptAdapter, 'restore_originals', restoring)
    monkeypatch.setattr(server, '_build_connection', lambda: connection(index))
    mcp = server.build_server()
    listed = _tool_data(mcp, 'list_sources', {})
    views = _tool_data(mcp, 'list_sources', {'subject': 'views'})
    assert not restored
    assert listed['fec_release']['status_counts'] == {'compatible': len(specs)}
    assert {name for entry in views['deferred_views'] for name in entry['views']} >= {spec.view.name for spec in specs}
    assert sum(entry['count'] for entry in listed['deferred_views']) == sum(len(entry['views']) for entry in views['deferred_views'])
    described = _tool_data(mcp, 'describe_table', {'table': specs[0].view.name, 'detail': True})
    assert not restored
    assert specs[0].view.name not in {*views['unavailable_tables'], *views['unavailable_views']}
    assert described['available'] is False
    assert described['relationship']['status'] == 'deferred'
    assert described['metadata']['query_admission'] == 'pending_receipt_rows'
    assert described['columns'] == []
    assert server._cached_connection is None
    result = _tool_data(mcp, 'query_sql', {'sql': f'SELECT * FROM {specs[0].view.name}'})
    assert _records(result) == [{'id': 1}]
    assert 'fec_receipts' in restored
    assert server._cached_connection is not server._cached_discovery_connection
    query_connection = getattr(server, '_cached_connection')
    assert query_connection is not None
    registry = server._connection_relationships(query_connection)
    assert registry[specs[0].view.name]['status'] == 'available'
    assert all(registry[spec.view.name]['status'] == 'deferred' for spec in specs[1:])


def test_native_discovery_keeps_pins_without_receipt_row_replay(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    original = etl_receipts.validate_receipt_bundle
    admissions = []
    def validate(*args, **kwargs):
        admissions.append(kwargs['generation_id'])
        return original(*args, **kwargs)
    monkeypatch.setattr(etl_receipts, 'validate_receipt_bundle', validate)
    mcp = server.build_server()
    description = _tool_data(mcp, 'describe_table', {'table': 'members'})
    assert description['available'] and not admissions
    assert 'receipt row joins remain pending' in description['publication']['verification']
    query = _tool_data(mcp, 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})
    assert _records(query) == [{'bioguide_id': 'X'}] and admissions
    # Descriptor-only mode still refuses changed exact bytes.
    server._reset_connection_cache()
    selected = local_data.local_selection(tmp_path)
    selected.native['members'].subjects[0].write_bytes(b'changed')
    with pytest.raises(Exception, match='changed|Parquet'):
        _tool_data(mcp, 'describe_table', {'table': 'members'})


@pytest.mark.parametrize('cancellation', ['deadline', 'external'])
def test_cold_native_hash_admission_cancelled_never_enters_cache(tmp_path, monkeypatch, cancellation):
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    original = selected_generations.stream_sha256
    entered, ended = Event(), Event()
    def slow_hash(stream):
        entered.set()
        try:
            while True:
                sleep(.005)
                checkpoint()
        finally:
            ended.set()
    monkeypatch.setattr(selected_generations, 'stream_sha256', slow_hash)
    bound = RequestBound(.10 if cancellation == 'deadline' else 5, 'cold input admission cancelled')
    timer = Timer(.08, bound.cancel) if cancellation == 'external' else None
    started = monotonic()
    if timer:
        timer.start()
    try:
        with pytest.raises(TimeoutError, match='cold input admission'):
            with bound.scope():
                server._get_connection()
    finally:
        if timer:
            timer.cancel()
    assert entered.is_set() and ended.is_set()
    assert monotonic() - started < 1
    assert server._cached_connection is None
    monkeypatch.setattr(selected_generations, 'stream_sha256', original)
    assert server._get_connection().execute('SELECT bioguide_id FROM members').fetchall() == [('X',)]


def test_request_timeout_covers_cold_build_and_worker_stops(tmp_path, monkeypatch):
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    # Give the worker a bounded startup window under Linux emulation, then
    # expire the real request bound inside the instrumented admission phase.
    monkeypatch.setattr(server, 'STATEMENT_TIMEOUT_SECONDS', 30)
    entered_at = []
    stopped = Event()
    def slow_hash(stream):
        held = current_bound()
        assert held is not None
        entered_at.append(monotonic())
        held.deadline = entered_at[0] + .10
        try:
            while True:
                sleep(.005)
                checkpoint()
        finally:
            stopped.set()
    monkeypatch.setattr(selected_generations, 'stream_sha256', slow_hash)
    with pytest.raises(Exception, match='including input admission'):
        _tool_data(server.build_server(), 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})
    assert stopped.wait(1)
    assert entered_at and monotonic() - entered_at[0] < 1.5
    assert server._cached_connection is None


def test_hosted_discovery_budget_returns_terminal_error_and_same_client_recovers(monkeypatch, record_property):
    from starlette.testclient import TestClient
    from tests.test_mcp_query_results import call
    from tests.test_mcp_server import _make_local_connection

    _make_local_connection(monkeypatch, server)
    monkeypatch.setattr(server, "STATEMENT_TIMEOUT_SECONDS", 600)
    monkeypatch.setattr(server, "STATEMENT_TIMEOUT", "600s")
    original = server._get_connection
    stalled, stopped = [True], Event()
    query_budget = []

    def connection():
        if stalled[0]:
            stop_at = monotonic() + 18
            try:
                while monotonic() < stop_at:
                    sleep(.005)
                    checkpoint()
                raise AssertionError("Discovery did not enforce its request deadline")
            finally:
                stopped.set()
        held = current_bound()
        assert held is not None and held.deadline is not None
        query_budget.append(held.deadline - monotonic())
        return original()

    monkeypatch.setattr(server, "_get_connection", connection)
    with TestClient(server.build_app()) as http:
        started = monotonic()
        failed = call(http, "list_sources", {})
        terminal_seconds = monotonic() - started
        record_property("terminal_error_seconds", terminal_seconds)
        assert 14 <= terminal_seconds < 18
        assert failed["isError"] is True
        assert "15s timeout, including input admission" in failed["content"][0]["text"]
        cleanup_started = monotonic()
        assert stopped.wait(1)
        record_property("remaining_cleanup_seconds", monotonic() - cleanup_started)
        assert server._cached_discovery_connection is None
        stalled[0] = False
        recovery_started = monotonic()
        recovered = call(http, "list_sources", {})
        record_property("recovery_seconds", monotonic() - recovery_started)
        assert recovered["isError"] is False
        assert recovered["structuredContent"]["subjects"]
        assert 0 < query_budget[-1] <= 15
        queried = call(http, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})
        assert queried["isError"] is False
        assert _records(queried["structuredContent"]) == [{"docket_count": 1}]
        assert query_budget[-1] > 590
        record_property("query_budget_remaining_seconds", query_budget[-1])


def test_external_tool_cancellation_stops_cold_worker(tmp_path, monkeypatch):
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    monkeypatch.setattr(server, 'STATEMENT_TIMEOUT_SECONDS', 5)
    entered, stopped = Event(), Event()
    def slow_hash(stream):
        entered.set()
        try:
            while True:
                sleep(.005)
                checkpoint()
        finally:
            stopped.set()
    monkeypatch.setattr(selected_generations, 'stream_sha256', slow_hash)
    mcp = server.build_server()
    async def request():
        call = asyncio.create_task(mcp.call_tool('query_sql', {'sql': 'SELECT bioguide_id FROM members'}))
        while not entered.is_set():
            await asyncio.sleep(.005)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
    asyncio.run(request())
    assert stopped.wait(1)
    assert server._cached_connection is None


def test_bulk_admission_sql_interrupts_and_removes_private_work(tmp_path):
    with pytest.raises(TimeoutError, match='bulk admission cancelled'):
        with RequestBound(.10, 'bulk admission cancelled').scope():
            with etl_bulk.bulk_connection(tmp_path) as (con, _):
                con.execute('SELECT sum(hash(i)) FROM range(10000000000) AS t(i)').fetchone()
    assert list(tmp_path.iterdir()) == []
    with etl_bulk.bulk_connection(tmp_path) as (con, _):
        assert con.execute('SELECT 1').fetchone() == (1,)


def test_discovery_cache_age_does_not_restart_on_every_read(monkeypatch):
    token = server._discovery_only.set(True)
    try:
        values = iter([10., 20., 311., 311.])
        monkeypatch.setattr(server, '_monotonic', lambda: next(values))
        monkeypatch.setattr(server, '_build_connection', lambda: 'old')
        monkeypatch.setattr(server, '_refreshed', lambda old: 'new')
        assert server._get_connection() == 'old'
        assert server._get_connection() == 'old'
        assert server._cached_discovery_at == 10
        assert server._get_connection() == 'new'
    finally:
        server._discovery_only.reset(token)
        server._reset_connection_cache()


def test_discovery_refresh_timeout_preserves_connection_and_releases_owner(monkeypatch):
    import duckdb

    now = [10.0]
    monkeypatch.setattr(server, "_monotonic", lambda: now[0])
    con = duckdb.connect()
    monkeypatch.setattr(server, "_build_connection", lambda: con)

    def timeout(_):
        raise TimeoutError("refresh timed out")

    monkeypatch.setattr(server, "_refreshed", timeout)
    discovery = server._discovery_only.set(True)
    try:
        assert server._get_connection() is con
        now[0] += server._CONNECTION_TTL_SECONDS + 1
        with pytest.raises(TimeoutError, match="refresh timed out"):
            server._get_connection()
        assert server._pointer_refresh_lock.acquire(blocking=False)
        server._pointer_refresh_lock.release()
        assert server._get_connection() is con
    finally:
        server._discovery_only.reset(discovery)


def test_old_receipt_only_discovery_is_hidden_without_payload_restore(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from tests.test_fec_historical_policy_readback import old_pair
    from mcp.server.mcpserver.exceptions import ToolError

    row = {'collection_id': 'old-collection', 'source_family': 'bulk', 'record_count': 1,
           'conversion_inputs': {'collection_id': 'old-collection', 'source_family': 'bulk', 'record_count': 1}}
    _, subject, receipt = old_pair(tmp_path / 'bundle', 'fec_collections', row)
    assert subject is None
    root = tmp_path / 'state'
    remember_selection(root, [SelectedDataset('fec_collections', (), receipt, 'historical')])
    def refuse_restore(*args, **kwargs):
        pytest.fail('Discovery replayed receipt payloads or restored financial rows')
    monkeypatch.setattr(etl_receipts, 'select_receipts', refuse_restore)
    monkeypatch.setattr(etl_receipts, 'validate_receipt_bundle', refuse_restore)
    monkeypatch.setattr(ReceiptAdapter, 'restore_originals', refuse_restore)
    selection = local_data.local_selection(root, admit_rows=False)
    assert 'fec_collections' not in selection.files
    assert selection.native['fec_collections'].receipts == receipt
    monkeypatch.setattr(server, 'DATA_DIR', root)
    monkeypatch.setattr(server, 'TABLES', ())
    mcp = server.build_server()
    assert 'fec_collections' not in {item['table'] for item in _listed(mcp)}
    with pytest.raises(ToolError, match='Unknown table'):
        _tool_data(mcp, 'describe_table', {'table': 'fec_collections'})
    assert server._cached_connection is None


def _remote_fixture(tmp_path, monkeypatch, index):
    """Run real remote connection construction against only tiny local fixture URLs."""
    monkeypatch.setattr(server, "DATA_DIR", None)
    monkeypatch.setattr(server, "R2_BASE_URL", str(tmp_path))
    monkeypatch.setattr(server, "TABLES", ())
    monkeypatch.setattr(server, "load_public_http", lambda *args: None)
    monkeypatch.setattr(server, "INTERACTIVE_HTTP_RETRIES", {})
    monkeypatch.setattr(server, "_read_publication", lambda: server._Publication(index, None, None))
    admitted = []
    original = server._admit_remote_source_view

    def admit(cursor, source):
        admitted.append(source.name)
        return original(cursor, source)

    monkeypatch.setattr(server, "_admit_remote_source_view", admit)
    return admitted


@pytest.fixture
def scoped_remote(tmp_path, monkeypatch):
    import duckdb
    from spicy_regs.sources.publication import file_identity

    queries = {
        "lobbying_activities": "SELECT 'filing-1'::VARCHAR filing_uuid, 0::BIGINT activity_index, "
        "[{'id': 'agency-1', 'name': 'Native agency'}] government_entities",
        "other_rows": "SELECT 7::INTEGER id",
        "broken_rows": "SELECT 8::INTEGER id",
    }
    tables = {}
    with duckdb.connect() as con:
        for name, query in queries.items():
            path = tmp_path / (name + ".parquet")
            con.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(path)])
            columns = [list(row[:2]) for row in con.execute("DESCRIBE " + query).fetchall()]
            identity = file_identity(path)
            tables[path.name] = {
                "sha256": identity["sha256"],
                "byteSize": identity["bytes"],
                "rows": 1,
                "columns": columns,
            }
    tables["broken_rows.parquet"]["columns"] = [["wrong", "VARCHAR"]]
    index = {
        "families": {
            "fixture": {
                "artifactDigest": "sha256:" + "a" * 64,
                "prefix": "",
                "tables": tables,
                "etlReceipts": {
                    "key": "unrequested-missing.parquet",
                    "datasets": [],
                    "generationId": "fixture",
                    "rows": 1,
                    "byteSize": 1,
                    "sha256": "sha256:" + "b" * 64,
                },
            }
        }
    }
    monkeypatch.setattr(server, "FEC_QUALIFIED_VIEWS", ())
    admitted = _remote_fixture(tmp_path, monkeypatch, index)
    return server.build_server(), index, admitted


def test_remote_query_registers_only_lobbying_dependency_and_keeps_security(scoped_remote, tmp_path):
    mcp, index, admitted = scoped_remote
    name = "lobbying_contacted_entities_occurrences"
    result = _tool_data(mcp, "query_sql", {"sql": f"SELECT target_key FROM {name} LIMIT 1"})
    assert _records(result) == [{"target_key": "agency-1"}]
    assert admitted == ["lobbying_activities"]
    assert result["publication"][name]["rule_version"] == "lda-native-entities/1"
    assert (
        result["publication"][name]["input_publications"]["lobbying_activities"]["artifact_digest"]
        == index["families"]["fixture"]["artifactDigest"]
    )
    con = server._cached_connection
    assert con is not None
    assert server._connection_index(con) == index
    assert "etl_receipts" not in server._available_tables(con)
    assert con.execute("SELECT current_setting('enable_external_access')").fetchone() == (False,)
    with pytest.raises(Exception, match="disabled|permission|not allowed"):
        _tool_data(
            mcp,
            "query_sql",
            {"sql": f"SELECT * FROM lobbying_activities CROSS JOIN read_parquet('{tmp_path / 'other_rows.parquet'}')"},
        )
    assert admitted == ["lobbying_activities"]


def test_remote_discovery_uses_selected_descriptors_without_opening_managed_files(scoped_remote, monkeypatch):
    from mcp.server.mcpserver.exceptions import ToolError

    mcp, index, admitted = scoped_remote
    index["families"]["fixture"]["etlReceipts"]["columns"] = [["dataset", "VARCHAR"]]
    monkeypatch.setattr(server, "_bind_archive", lambda _: (None, None))
    monkeypatch.setattr(server, "_family_parents", lambda *args: {})
    monkeypatch.setattr(server, "load_public_http", lambda *args: pytest.fail("No files needed for this catalog"))
    listing = _tool_data(mcp, "list_sources", {})
    assert set().union(*(set(group["tables"]) for group in listing["subjects"])) == {
        "lobbying_activities", "other_rows", "broken_rows", "etl_receipts",
    }
    assert listing["etl_receipts"]["available"]
    assert "Selected publication" in listing["discovery_basis"]
    described = _tool_data(mcp, "describe_table", {"table": "broken_rows"})
    assert described["schema_basis"] == "selected_publication"
    assert described["columns"] == [{"column_name": "wrong", "column_type": "VARCHAR", "description": None}]
    assert described["publication"]["rows"] == 1
    receipts = _tool_data(mcp, "describe_table", {"table": "etl_receipts"})
    assert receipts["columns"][0]["column_name"] == "dataset"
    assert receipts["publication"]["rows"] == 1
    assert admitted == []
    discovery = server._cached_discovery_connection
    assert discovery is not None
    assert not {"broken_rows", "etl_receipts"} & server._registered_tables(discovery)
    assert server._cached_connection is None

    monkeypatch.setattr(server, "load_public_http", lambda *args: None)
    with pytest.raises(ToolError, match="Published schema differs"):
        _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM broken_rows"})
    assert admitted == ["broken_rows"]
    assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT * FROM other_rows"})) == [{"id": 7}]
    assert server._cached_connection is not discovery


def test_remote_discovery_keeps_actual_schema_fallback_for_unpinned_tables(scoped_remote, tmp_path, monkeypatch):
    import shutil

    mcp, index, admitted = scoped_remote
    del index["families"]["fixture"]["etlReceipts"]
    monkeypatch.setattr(server, "TABLES", ("legacy_rows",))
    monkeypatch.setattr(server, "_bind_archive", lambda _: (None, None))
    shutil.copyfile(tmp_path / "other_rows.parquet", tmp_path / "legacy_rows.parquet")
    listing = _tool_data(mcp, "list_sources", {})
    assert any("legacy_rows" in group["tables"] for group in listing["subjects"])
    described = _tool_data(mcp, "describe_table", {"table": "legacy_rows"})
    assert described["columns"] == [{"column_name": "id", "column_type": "INTEGER", "description": None}]
    assert "schema_basis" not in described
    assert described["publication"]["status"] == "legacy_unversioned"
    assert admitted == ["legacy_rows"]


def test_remote_base_expansion_keeps_pins_and_failed_replacement(scoped_remote, monkeypatch):
    mcp, index, admitted = scoped_remote
    _tool_data(mcp, "query_sql", {"sql": "SELECT filing_uuid FROM lobbying_activities LIMIT 1"})
    original = server._cached_connection
    assert original is not None
    monkeypatch.setattr(server, "_read_publication", lambda: pytest.fail("Expansion must reuse captured pins"))
    with pytest.raises(Exception, match="Published schema differs"):
        _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM broken_rows"})
    assert server._cached_connection is original
    assert original.execute("SELECT filing_uuid FROM lobbying_activities").fetchone() == ("filing-1",)
    assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT id FROM other_rows"})) == [{"id": 7}]
    expanded = server._cached_connection
    assert expanded is not None and expanded is not original and server._connection_index(expanded) == index
    before = list(admitted)
    _tool_data(mcp, "query_sql", {"sql": "SELECT filing_uuid FROM lobbying_activities"})
    assert server._cached_connection is expanded and admitted == before


def test_cancelled_remote_base_expansion_keeps_prior_connection(scoped_remote, monkeypatch):
    mcp, _, _ = scoped_remote
    _tool_data(mcp, "query_sql", {"sql": "SELECT filing_uuid FROM lobbying_activities"})
    original = server._cached_connection
    admit = server._admit_remote_source_view
    stopped = Event()

    def blocked(cursor, source):
        if source.name == "other_rows":
            current_bound().cancel()
            try:
                checkpoint()
            finally:
                stopped.set()
        return admit(cursor, source)

    monkeypatch.setattr(server, "_admit_remote_source_view", blocked)
    with pytest.raises(Exception, match="including input admission"):
        _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM other_rows"})
    assert stopped.wait(1)
    assert server._cached_connection is original
    assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT filing_uuid FROM lobbying_activities"})) == [
        {"filing_uuid": "filing-1"}
    ]


def test_remote_scoped_fec_query_still_restores_exact_receipt_inputs(tmp_path, monkeypatch):
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.sources import publication
    from tests.test_mcp_fec_release import configure

    specs, index, *_ = configure(tmp_path, monkeypatch)
    bucket = tmp_path / "bucket"
    for entry in index["families"].values():
        for key in entry["tables"]:
            for member in publication.table_members(index, key):
                target = bucket / member.path
                target.parent.mkdir(parents=True, exist_ok=True)
                publication.fetch_member("fixture", member, target)
    admitted = _remote_fixture(bucket, monkeypatch, index)
    restored = []
    original = ReceiptAdapter.restore_originals

    def restoring(self, table, *args, **kwargs):
        restored.append(table)
        return original(self, table, *args, **kwargs)

    monkeypatch.setattr(ReceiptAdapter, "restore_originals", restoring)
    result = _tool_data(server.build_server(), "query_sql", {"sql": f"SELECT * FROM {specs[0].view.name}"})
    assert _records(result) == [{"id": 1}]
    assert set(admitted) == {"fec_receipts", "fec_filings"}
    assert set(restored) == {"fec_receipts", "fec_filings"}
    assert result["publication"][specs[0].view.name]["release_compatibility"]["status"] == "compatible"


def test_explicit_remote_receipts_keep_all_families_and_catalog_queries_expand(scoped_remote, tmp_path):
    import duckdb
    from spicy_regs.sources.publication import file_identity

    mcp, index, admitted = scoped_remote
    index["families"]["fixture"]["tables"].pop("broken_rows.parquet")
    index["families"]["second"] = {"artifactDigest": "sha256:" + "c" * 64, "prefix": "", "tables": {}}
    with duckdb.connect() as con:
        for family, entry in index["families"].items():
            path = tmp_path / (family + "-receipts.parquet")
            con.execute(f"COPY (SELECT '{family}'::VARCHAR dataset) TO ? (FORMAT PARQUET)", [str(path)])
            identity = file_identity(path)
            entry["etlReceipts"] = {
                "key": path.name,
                "sha256": identity["sha256"],
                "byteSize": identity["bytes"],
                "rows": 1,
                "columns": [["dataset", "VARCHAR"]],
                "generationId": family,
                "datasets": [family],
            }
    _tool_data(mcp, "query_sql", {"sql": "SELECT filing_uuid FROM lobbying_activities"})
    result = _tool_data(mcp, "query_sql", {"sql": "SELECT dataset FROM etl_receipts ORDER BY dataset"})
    assert _records(result) == [{"dataset": "fixture"}, {"dataset": "second"}]
    assert set(result["publication"]["etl_receipts"]["families"]) == {"fixture", "second"}
    assert "other_rows" not in admitted
    result = _tool_data(mcp, "query_sql", {"sql": "SHOW TABLES"})
    assert "other_rows" in {row["name"] for row in _records(result)}
    connection = server._cached_connection
    assert connection is not None and server._pinned_record(connection, "_spicy_query_scope") is None
