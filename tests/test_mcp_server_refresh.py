"""Warm readers discover new publications without unbounded missing-name polls."""

import threading

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server as server
from spicy_regs.sources import publication
from tests.generation_fakes import Store
from tests.test_generation_mcp import connection_fixture
from tests.test_generation_publication import build, publish
from tests.test_mcp_server import _Publisher, _make_local_connection, _moved, _records, _tool_data


@pytest.fixture(autouse=True)
def reset_connections():
    server._reset_connection_cache()
    yield
    server._reset_connection_cache()


@pytest.fixture
def clock(monkeypatch):
    now = [10.0]
    monkeypatch.setattr(server, "_monotonic", lambda: now[0])
    return now


@pytest.fixture
def published_files(tmp_path, monkeypatch):
    """Run the real connection builder over immutable Parquet generations; replace only transport."""
    store = Store()
    old, _ = build(tmp_path, "old", "existing", ("warm_source.parquet", "sibling_source.parquet"), "old")
    old_index = publish(store, old)
    changed, _ = build(tmp_path, "changed", "existing", ("warm_source.parquet", "sibling_source.parquet"), "new")
    new_index = publish(store, changed, old_index)
    added, _ = build(tmp_path, "added", "added", ("new_source.parquet",), "new-family")
    new_index = publish(store, added, new_index)
    locations = {
        publication.single_member(index, key).path: directory / key
        for index, directory, key in (
            (old_index, old, "warm_source.parquet"),
            (old_index, old, "sibling_source.parquet"),
            (new_index, changed, "warm_source.parquet"),
            (new_index, changed, "sibling_source.parquet"),
            (new_index, added, "new_source.parquet"),
        )
    }
    monkeypatch.setattr(server, "DATA_DIR", None)
    connection_fixture(monkeypatch, old_index, locations, tables=("warm_source", "sibling_source", "new_source"))
    monkeypatch.setattr(server, "load_public_http", lambda *args: None)
    state = _Publisher(server._Publication(old_index, None))

    def read():
        state.reads += 1
        return state.publication

    monkeypatch.setattr(server, "_read_publication", read)
    return state, server._Publication(new_index, None)


@pytest.mark.parametrize("tool_name,subject", [
    ("query_sql", None), ("describe_table", None),
    ("list_sources", None), ("list_sources", server.UNASSIGNED_SUBJECT),
])
def test_new_family_is_visible_before_ttl_and_old_cursor_keeps_its_pins(published_files, clock, tool_name, subject):
    state, moved = published_files
    mcp = server.build_server()
    if tool_name == "query_sql":
        assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT id FROM warm_source"})) == [{"id": "old"}]
        first = server._cached_connection
    else:
        _tool_data(mcp, "list_sources", {})
        first = server._cached_discovery_connection
    assert first is not None
    old_cursor = first.cursor()
    assert "new_source" not in server._available_tables(old_cursor)
    state.publication, clock[0] = moved, 11.0

    arguments = {"sql": "SELECT id FROM new_source LIMIT 1"} if tool_name == "query_sql" else (
        {"table": "new_source"} if tool_name == "describe_table" else {"subject": subject})
    result = _tool_data(mcp, tool_name, arguments)
    if tool_name == "query_sql":
        assert _records(result) == [{"id": "new-family"}]
        current = server._cached_connection
    else:
        if tool_name == "describe_table":
            assert result["available"]
        elif subject is None:
            assert sum(len(group["tables"]) for group in result["subjects"]) == 3
        else:
            assert "new_source" in {row["table"] for group in result["subjects"] for row in group["tables"]}
        current = server._cached_discovery_connection
    assert current is not first
    assert current is not None
    assert server._pinned_publication(current) == moved
    assert state.reads == 2  # initial pointer read plus one early check
    if tool_name == "query_sql":
        assert old_cursor.execute("SELECT id FROM warm_source").fetchall() == [("old",)]
    else:
        assert "warm_source" in server._available_tables(old_cursor)
        assert "warm_source" not in server._registered_tables(old_cursor)
    assert server._pinned_publication(old_cursor) != moved
    old_cursor.close()
    with pytest.raises(ToolError, match="read-only"):
        _tool_data(mcp, "query_sql", {"sql": "DROP VIEW warm_source"})


def test_unknown_names_and_existing_tables_do_not_poll_pointers(monkeypatch, clock):
    state = _make_local_connection(monkeypatch, server)
    mcp = server.build_server()
    _tool_data(mcp, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})
    _tool_data(mcp, "describe_table", {"table": "agency_stats"})
    assert state.reads == 2  # independently pinned query and discovery caches
    for number in range(12):
        clock[0] += 6.0  # even past the early cooldown, unregistered names cannot poll
        with pytest.raises(ToolError, match="does not exist"):
            _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM unknown_{number}"})
        with pytest.raises(ToolError, match="Unknown table"):
            _tool_data(mcp, "describe_table", {"table": f"unknown_{number}"})
        _tool_data(mcp, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})
        _tool_data(mcp, "describe_table", {"table": "agency_stats"})
    assert state.reads == 2


@pytest.mark.parametrize("failure", ["unchanged", "pointer", "build", "timeout"])
def test_missing_registered_tables_share_cooldown_and_refresh_failure_preserves_service(monkeypatch, clock, failure):
    state = _make_local_connection(monkeypatch, server)
    mcp = server.build_server()
    _tool_data(mcp, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})
    _tool_data(mcp, "list_sources", {})
    first = server._cached_connection
    if failure == "pointer":
        state.publication = RuntimeError("pointer unavailable")
    elif failure in ("build", "timeout"):
        state.publication = _moved(server)
        state.fail = TimeoutError("bounded refresh timed out") if failure == "timeout" else RuntimeError("member unavailable")
    clock[0] = 11.0
    for _ in range(4):
        with pytest.raises(ToolError):
            _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM ofac_snapshots"})
        _tool_data(mcp, "describe_table", {"table": "ofac_snapshots"})
        _tool_data(mcp, "list_sources", {})
    assert state.reads == 3
    assert server._cached_connection is first
    assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})) == [{"docket_count": 1}]
    # Distinct known missing names still share the same cooldown.
    with pytest.raises(ToolError):
        _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM grants_opportunity_observations"})
    assert state.reads == 3
    state.publication, state.fail = _moved(server), None
    clock[0] += server._EARLY_REFRESH_COOLDOWN_SECONDS
    _tool_data(mcp, "list_sources", {})
    assert state.reads == 4
    assert server._cached_discovery_connection is not None
    assert server._pinned_publication(server._cached_discovery_connection) == state.publication


def test_query_and_discovery_share_one_inflight_refresh_owner(monkeypatch, clock):
    state = _make_local_connection(monkeypatch, server)
    mcp = server.build_server()
    _tool_data(mcp, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})
    _tool_data(mcp, "list_sources", {})
    first, discovery = server._cached_connection, server._cached_discovery_connection
    state.publication, clock[0] = _moved(server), 11.0
    entered, release = threading.Event(), threading.Event()
    build_connection = server._build_connection

    def waiting_build(publication=None):
        entered.set()
        assert release.wait(10)
        return build_connection(publication)

    monkeypatch.setattr(server, "_build_connection", waiting_build)
    outcomes = []

    def refresh():
        with pytest.raises(ToolError, match="does not exist"):
            _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM ofac_snapshots"})
        outcomes.append(True)

    worker = threading.Thread(target=refresh)
    worker.start()
    try:
        assert entered.wait(10)
        assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT docket_count FROM agency_stats"})) == [{"docket_count": 1}]
        _tool_data(mcp, "list_sources", {})
        assert server._cached_discovery_connection is discovery
        assert server._cached_connection is first
        assert state.reads == 3
    finally:
        release.set()
        worker.join(10)
    assert outcomes == [True]
    assert server._cached_connection is not first
    assert state.builds == 3


def test_expired_query_expands_old_snapshot_while_discovery_owns_refresh(published_files, clock, monkeypatch):
    state, moved = published_files
    mcp = server.build_server()
    _tool_data(mcp, "query_sql", {"sql": "SELECT id FROM warm_source"})
    _tool_data(mcp, "list_sources", {})
    first = server._cached_connection
    state.publication, clock[0] = moved, 311.0
    entered, release = threading.Event(), threading.Event()
    build_connection = server._build_connection

    def waiting_discovery(publication=None):
        if server._discovery_only.get():
            entered.set()
            assert release.wait(10)
        return build_connection(publication)

    monkeypatch.setattr(server, "_build_connection", waiting_discovery)
    replies = []
    worker = threading.Thread(target=lambda: replies.append(_tool_data(mcp, "list_sources", {})))
    worker.start()
    try:
        assert entered.wait(10)
        result = _tool_data(mcp, "query_sql", {"sql": "SELECT id FROM sibling_source"})
        assert _records(result) == [{"id": "old"}]
        assert server._cached_connection is not first
        assert server._cached_connection is not None
        assert server._pinned_publication(server._cached_connection) != moved
        assert state.reads == 3
    finally:
        release.set()
        worker.join(10)
    assert replies
    assert server._cached_discovery_connection is not None
    assert server._pinned_publication(server._cached_discovery_connection) == moved
