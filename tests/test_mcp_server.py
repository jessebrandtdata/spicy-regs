"""Tests for the Spicy Regs MCP server.

Covers the canonical MCP server implementation in ``spicy_regs.mcp_server``
(the single source of truth since the Vercel copy was retired).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import duckdb
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server


def test_jsonify_primitives_pass_through():
    assert mcp_server._jsonify(None) is None
    assert mcp_server._jsonify("hi") == "hi"
    assert mcp_server._jsonify(7) == 7
    assert mcp_server._jsonify(1.5) == 1.5
    assert mcp_server._jsonify(True) is True


def test_jsonify_coerces_non_json_types():
    j = mcp_server._jsonify
    assert j(date(2024, 1, 2)) == "2024-01-02"
    assert j(datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"
    assert j(Decimal("1.50")) == "1.50"
    assert j(UUID("12345678-1234-5678-1234-567812345678")) == ("12345678-1234-5678-1234-567812345678")
    assert j(b"\x00\xff") == "00ff"
    assert j([Decimal("1"), date(2024, 1, 1)]) == ["1", "2024-01-01"]
    assert j({"a": Decimal("2"), "b": [b"\xab"]}) == {"a": "2", "b": ["ab"]}


def test_resolve_r2_base_url_rejects_http(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "http://example.com")
    with pytest.raises(RuntimeError, match="https://"):
        mcp_server._resolve_r2_base_url()


def test_resolve_r2_base_url_rejects_injection_chars(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://evil.example/'); DROP")
    with pytest.raises(RuntimeError, match="illegal characters"):
        mcp_server._resolve_r2_base_url()


def test_resolve_r2_base_url_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("SPICY_REGS_R2_URL", "https://example.com/bucket/")
    assert mcp_server._resolve_r2_base_url() == "https://example.com/bucket"


def _tool_names(server) -> set[str]:
    """The names of the tools registered on ``server``."""
    tools = asyncio.run(server.list_tools())
    return {t.name for t in tools}


def test_build_server_registers_expected_tools():
    server = mcp_server.build_server()
    assert _tool_names(server) == {"list_sources", "describe_table", "query_sql", "resolve_document_citations", "lookup_agency",
                                   "read_receipt_fields", "connection_status"}


def _tool_data(server, name, arguments):
    """Call a tool and return its structured result."""
    return asyncio.run(server.call_tool(name, arguments)).structured_content


def _listed(server):
    """Every table entry list_sources lists, paged by subject, in the order the subjects are listed."""
    return [entry for group in _tool_data(server, "list_sources", {})["subjects"]
            for entry in _tool_data(server, "list_sources", {"subject": group["subject"]})["subjects"][0]["tables"]]


def _records(reply):
    """A query_sql reply's rows, which are arrays in the order of its columns, as one dict per row."""
    return [dict(zip(reply["columns"], row, strict=True)) for row in reply["rows"]]


def test_discovery_reports_actual_parquet_schema_and_dictionary_caveats(tmp_path, monkeypatch):
    """A readable old/changed artifact must not be described as the declared schema."""
    con = duckdb.connect()
    path = str(tmp_path / "org_committee_links.parquet").replace("'", "''")
    con.execute(
        "CREATE TABLE source_rows AS SELECT 'Example group' AS organization, "
        "'C00000001' AS committee_id, 'prefix' AS match_method, 'EXAMPLE GROUP' AS organization_norm, "
        "3::INTEGER AS committee_match_count, 'unmapped value' AS new_field"
    )
    con.execute(f"COPY source_rows TO '{path}' (FORMAT PARQUET)")
    con.execute(f"CREATE VIEW org_committee_links AS SELECT * FROM read_parquet('{path}')")
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    server = mcp_server.build_server()

    sources = _tool_data(server, "list_sources", {})
    declared = mcp_server._table_metadata()["org_committee_links"]
    assert _listed(server) == [
        {"table": "org_committee_links", "label": declared["label"], "coverage": declared["kind"], "rows": None}
    ]
    unavailable = _tool_data(server, "list_sources", {"subject": "views"})["unavailable_tables"]
    # The explicit page includes the sixteen owner-hidden tables; the default
    # count describes only its discoverable names (all sixteen are absent here).
    assert "fec_committees" in unavailable and sources["unavailable_table_count"] == len(unavailable) - 16

    result = _tool_data(server, "describe_table", {"table": "org_committee_links"})
    assert result["available"] is True
    assert result["schema_matches_declared"] is False
    # organization_norm is a receipt field of the native table; match_method is one of its columns.
    assert result["schema_differences"]["unexpected_columns"] == ["organization_norm", "new_field"]
    assert result["schema_differences"]["type_differences"] == [
        {"column": "committee_match_count", "declared": "BIGINT", "actual": "INTEGER"}
    ]
    assert "association_kind" in result["schema_differences"]["missing_columns"]
    assert "heuristic name matches" in result["metadata"]["data_quality"]
    assert result["metadata"]["identity_columns"] == ["organization", "committee_id"]
    actual_columns = {column["column_name"]: column for column in result["columns"]}
    assert "fec_committees.committee_id" in actual_columns["committee_id"]["description"]
    assert "`prefix`" in actual_columns["match_method"]["description"]
    assert actual_columns["new_field"]["description"] is None
    queried = _tool_data(server, "query_sql", {"sql": "SELECT committee_id FROM org_committee_links"})
    assert _records(queried) == [{"committee_id": "C00000001"}]

    unavailable = _tool_data(server, "describe_table", {"table": "fec_committees"})
    assert unavailable["available"] is False
    # An unavailable table still describes its declared columns, once.
    assert unavailable["columns"][0]["column_name"] == "committee_id"
    assert "declared_columns" not in unavailable
    assert unavailable["schema_matches_declared"] is None
    assert unavailable["schema_differences"] is None


def test_describe_exposes_provider_identity_without_importing_provider(monkeypatch):
    """Source-reader definitions are bundled; reading metadata does not import them."""
    import builtins

    original_import = builtins.__import__

    def without_provider(name, *args, **kwargs):
        if name == "spicy_docs" or name.startswith("spicy_docs."):
            raise AssertionError("MCP metadata tried to import source readers")
        return original_import(name, *args, **kwargs)

    con = duckdb.connect()
    con.execute("CREATE TABLE members (bioguide_id VARCHAR, fec_ids VARCHAR[])")
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    monkeypatch.setattr(builtins, "__import__", without_provider)
    mcp_server._table_metadata.cache_clear()
    result = _tool_data(mcp_server.build_server(), "describe_table", {"table": "members"})
    assert result["metadata"]["identity_columns"] == ["bioguide_id"]
    assert result["metadata"]["grain"]
    assert "FEC" in next(c["description"] for c in result["columns"] if c["column_name"] == "fec_ids")


def test_describe_unknown_table_refuses_names_outside_declarations_and_snapshot(monkeypatch):
    # Managed generations can introduce an output before dictionary prose is
    # adopted, so validity now includes the captured connection's admitted names.
    con = duckdb.connect()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    with pytest.raises(ToolError, match="Unknown table 'not_a_table'.*list_sources"):
        _tool_data(mcp_server.build_server(), "describe_table", {"table": "not_a_table"})
    con.close()


def test_local_directory_runs_actual_connection_without_remote_fallback(tmp_path, monkeypatch):
    con = duckdb.connect()
    target = str(tmp_path / "fec_committees.parquet").replace("'", "''")
    con.execute(
        f"COPY (SELECT 'C00000001' AS committee_id, 'Example committee' AS name) TO '{target}' (FORMAT PARQUET)"
    )
    con.close()
    monkeypatch.setenv("SPICY_REGS_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(mcp_server, "DATA_DIR", mcp_server._resolve_data_dir())

    def no_catalog():
        raise AssertionError("Local mode must not access R2 catalog configuration")

    monkeypatch.setattr(mcp_server, "_resolve_catalog_config", no_catalog)
    from spicy_regs.sources import publication

    monkeypatch.setattr(
        publication, "load_index", lambda url: (_ for _ in ()).throw(AssertionError("Remote index read"))
    )
    loaded = []
    apply_security = mcp_server._apply_security_settings

    def inspect_then_lock(con, allowed_paths=None):
        loaded.extend(con.execute(
            "SELECT extension_name FROM duckdb_extensions() WHERE loaded AND extension_name IN ('httpfs', 'iceberg')"
        ).fetchall())
        apply_security(con, allowed_paths)

    monkeypatch.setattr(mcp_server, "_apply_security_settings", inspect_then_lock)
    server = mcp_server.build_server()
    sources = _tool_data(server, "list_sources", {})
    assert sources["source"] == "local"
    assert sources["base_path"] == str(tmp_path)
    assert "base_url" not in sources
    assert [entry["table"] for entry in _listed(server)] == ["fec_committees"]
    assert loaded == []
    described = _tool_data(server, "describe_table", {"table": "fec_committees"})
    assert described["available"] is True
    assert described["source"] == "local"
    assert described["publication"] == {
        "status": "local_unversioned", "coverage": mcp_server._table_metadata()["fec_committees"]["kind"]
    }
    queried = _tool_data(server, "query_sql", {"sql": "SELECT committee_id FROM fec_committees"})
    assert queried["source"] == "local"
    assert _records(queried) == [{"committee_id": "C00000001"}]
    with pytest.raises(ToolError, match="read-only"):
        _tool_data(server, "query_sql", {"sql": "DROP VIEW fec_committees"})


@pytest.mark.parametrize("directory", ["", "/nonexistent-spicy-regs-directory"])
def test_local_directory_configuration_refuses_missing_input(monkeypatch, directory):
    monkeypatch.setenv("SPICY_REGS_DATA_DIR", directory)
    with pytest.raises(RuntimeError, match="SPICY_REGS_DATA_DIR"):
        mcp_server._resolve_data_dir()


# --- catalog config resolution ----------------------------------------------

_CATALOG_ENV = ("R2_CATALOG_URI", "R2_CATALOG_WAREHOUSE", "R2_CATALOG_TOKEN")


def test_resolve_catalog_config_none_when_unset(monkeypatch):
    module = mcp_server
    for var in (*_CATALOG_ENV, "R2_CATALOG_NAMESPACE"):
        monkeypatch.delenv(var, raising=False)
    assert module._resolve_catalog_config() is None


def test_resolve_catalog_config_partial_is_none(monkeypatch):
    """Missing any one of the three required vars => disabled (fall back)."""
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.delenv("R2_CATALOG_TOKEN", raising=False)
    assert module._resolve_catalog_config() is None


def test_resolve_catalog_config_full(monkeypatch):
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "secret-token")
    monkeypatch.delenv("R2_CATALOG_NAMESPACE", raising=False)
    config = module._resolve_catalog_config()
    assert config == {
        "uri": "https://catalog.example/x",
        "warehouse": "wh",
        "token": "secret-token",
        "namespace": module.DEFAULT_CATALOG_NAMESPACE,
    }


def test_resolve_catalog_config_empty_namespace_defaults(monkeypatch):
    """An empty R2_CATALOG_NAMESPACE (e.g. an unset GH secret -> "") -> default."""
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "t")
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "")
    config = module._resolve_catalog_config()
    assert config is not None
    assert config["namespace"] == module.DEFAULT_CATALOG_NAMESPACE


def test_resolve_catalog_config_rejects_injection(monkeypatch):
    """Values are inlined into CREATE SECRET / ATTACH, so quotes are rejected."""
    module = mcp_server
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/x")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh'); DROP")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "t")
    with pytest.raises(RuntimeError, match="illegal characters"):
        module._resolve_catalog_config()


# --- Connection sandbox -----------------------------------------------------
#
# These exercise the real ``_apply_security_settings`` pragmas, which is where
# both shipped runtime crashes lived (the bogus ``statement_timeout`` SET and
# the spill-to-disabled-LocalFileSystem error). They are hermetic: the sandbox
# is applied to a plain in-memory connection, so no httpfs install or network
# is needed. The live ``_build_connection`` + R2 path is covered by the integration test
# below.


def _sandboxed_connection(memory_limit: str | None = None):
    """A connection with the production read-only sandbox applied.

    ``memory_limit`` is set before the sandbox locks the configuration, so a
    test can force spilling behavior.
    """
    con = duckdb.connect()
    if memory_limit is not None:
        con.execute(f"SET memory_limit='{memory_limit}'")
    mcp_server._apply_security_settings(con)
    return con


def test_security_settings_apply_cleanly():
    """Every pragma must be accepted by the installed DuckDB (no Catalog Error).

    The original ``SET statement_timeout`` regression failed exactly here, on a
    parameter DuckDB does not recognize.
    """
    module = mcp_server
    con = duckdb.connect()
    module._apply_security_settings(con)  # must not raise


def test_sandbox_allows_in_memory_query():
    con = _sandboxed_connection()
    assert con.execute("SELECT 1 + 1").fetchone() == (2,)
    assert con.execute("SELECT count(*) FROM range(100)").fetchone() == (100,)


def test_sandbox_preserves_secret_settings_after_the_manager_is_initialized():
    with duckdb.connect() as con:
        con.execute("SET allow_persistent_secrets=false")
        con.execute("SELECT * FROM duckdb_secrets()").fetchall()
        mcp_server._apply_security_settings(con)
        assert con.execute("SELECT current_setting('allow_persistent_secrets')").fetchone() == (False,)


def test_sandbox_survives_temp_spill():
    """A query that exceeds memory must not raise the LocalFileSystem error.

    Regression for ``Permission Error: File system LocalFileSystem has been
    disabled by configuration``: ``temp_directory`` defaulted to a local
    ``.tmp`` that the sandbox forbids, so any spilling query crashed. With
    spilling disabled the query either runs in memory or fails with a clear
    out-of-memory error — never the confusing permission error.
    """
    con = _sandboxed_connection(memory_limit="20MB")
    spilling_sql = "SELECT i, count(*) AS c FROM range(3_000_000) r(i) GROUP BY i ORDER BY c, i DESC"
    try:
        con.execute(spilling_sql).fetchall()
    except duckdb.OutOfMemoryException:
        pass  # acceptable: spilling disabled, no local temp touched
    except duckdb.PermissionException as exc:
        pytest.fail(f"sandbox crashed a spilling query on local temp: {exc}")


def test_sandbox_does_not_disable_local_filesystem():
    """LocalFileSystem must stay enabled, or httpfs HTTPS reads break.

    Regression for ``Permission Error: File system LocalFileSystem has been
    disabled by configuration`` raised when a view binds: httpfs reads the
    system CA bundle off the local filesystem for the TLS handshake, so
    ``disabled_filesystems='LocalFileSystem'`` makes every R2 read fail. Guard
    against re-adding it.
    """
    con = _sandboxed_connection()
    disabled = con.execute("SELECT current_setting('disabled_filesystems')").fetchone()
    assert disabled[0] == ""


def test_sandbox_locks_configuration():
    """User SQL must not be able to relax the sandbox once it is applied."""
    con = _sandboxed_connection()
    with pytest.raises(duckdb.Error):
        con.execute("SET allow_unsigned_extensions=true")


# --- read-only statement guard ----------------------------------------------


READ_FORMS = [
    "SELECT 1",
    "WITH a AS (SELECT 1) SELECT * FROM a",
    "FROM range(3)",
    "DESCRIBE SELECT 1",
    "SHOW ALL TABLES",
    "SUMMARIZE SELECT 1",
    "VALUES (1), (2)",
    "/* lead */ SELECT 1",
    "SELECT 1 -- COPY (SELECT 1) TO '/tmp/x.csv'",
    "SELECT 'COPY (SELECT 1) TO /tmp/x.csv' AS s",
    # DuckDB rewrites PRAGMA's table-returning forms to SELECT (round 6, L9: the tool text now says so).
    "PRAGMA version",
    "PRAGMA show_tables",
    "PRAGMA table_info('dockets')",
    "PRAGMA database_size",
]

WRITE_FORMS = [
    ("EXPLAIN SELECT 1", "EXPLAIN"),
    ("EXPLAIN ANALYZE DELETE FROM harmless_fixture", "EXPLAIN"),
    ("EXPLAIN (ANALYZE) DELETE FROM harmless_fixture", "EXPLAIN"),
    ("EXPLAIN /* nested statement */ ANALYZE SET enable_external_access=true", "EXPLAIN"),
    ("COPY (SELECT 1) TO '/tmp/probe.csv'", "COPY"),
    ("ATTACH '/tmp/probe.db' AS z", "ATTACH"),
    ("EXPORT DATABASE '/tmp/probe'", "EXPORT"),
    ("CREATE TABLE t (i INTEGER)", "CREATE"),
    ("CREATE VIEW v AS SELECT 1", "CREATE"),
    ("DROP TABLE IF EXISTS t", "DROP"),
    ("INSERT INTO t VALUES (1)", "INSERT"),
    ("UPDATE t SET i = 1", "UPDATE"),
    ("DELETE FROM t", "DELETE"),
    ("SET memory_limit='1GB'", "SET"),
    ("LOAD httpfs", "LOAD"),
    ("CALL pragma_version()", "CALL"),
    ("PREPARE p AS SELECT 1", "PREPARE"),
    ("BEGIN TRANSACTION", "TRANSACTION"),
    # PRAGMA's state-changing forms keep their own statement type, or parse as the SET they are.
    ("PRAGMA enable_profiling", "PRAGMA"),
    ("PRAGMA threads=4", "SET"),
    ("PRAGMA memory_limit='1GB'", "SET"),
]


@pytest.mark.parametrize("sql", READ_FORMS)
def test_read_forms_pass_the_guard(sql):
    """Every read shape a client might send must survive the allowlist.

    The allowlist is only {SELECT} because DuckDB's parser folds
    DESCRIBE/SHOW/SUMMARIZE/VALUES and the FROM-first shorthand into SELECT.
    If that ever stops holding, these are the cases that break.
    """
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, sql) is None


@pytest.mark.parametrize("sql,expected", WRITE_FORMS)
def test_write_forms_are_named_and_rejected(sql, expected):
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, sql) == expected


def test_import_database_is_refused_by_the_locked_connection_where_the_guard_cannot_see_it(tmp_path, monkeypatch):
    """DuckDB expands PRAGMA import_database while parsing, reading the directory's schema.sql: a file of SELECTs
    passes the statement guard on an open connection, so only the locked connection's file boundary refuses it."""
    (tmp_path / "schema.sql").write_text("SELECT 42;")
    (tmp_path / "load.sql").write_text("")
    statement = f"PRAGMA import_database('{tmp_path}')"
    assert mcp_server._first_write_statement(duckdb.connect(), statement) is None
    con = _sandboxed_connection()
    with pytest.raises(duckdb.PermissionException, match="schema.sql"):
        mcp_server._first_write_statement(con, statement)
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    with pytest.raises(ToolError, match="Permission Error"):
        _tool_data(mcp_server.build_server(), "query_sql", {"sql": statement})


def test_guard_catches_a_write_stacked_behind_a_select():
    """A trailing write must not ride in on a leading SELECT.

    ``execute`` runs every statement in the string but returns only the last
    result, so a stacked write would otherwise land silently.
    """
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, "SELECT 1; DROP TABLE t") == "DROP"


def test_guard_ignores_empty_sql():
    con = _sandboxed_connection()
    assert mcp_server._first_write_statement(con, "   ") is None


def test_guard_lets_parser_errors_through():
    """Malformed SQL keeps surfacing DuckDB's message, which names the token."""
    con = _sandboxed_connection()
    with pytest.raises(duckdb.ParserException):
        mcp_server._first_write_statement(con, "SELECT ((")


def test_query_sql_refuses_a_write_without_executing_it(monkeypatch, tmp_path):
    """End-to-end: the tool returns an error and the COPY never lands on disk."""
    module = mcp_server
    module._reset_connection_cache()
    _make_local_connection(monkeypatch, module)
    server = module.build_server()
    target = tmp_path / "written.csv"

    with pytest.raises(ToolError, match="read-only"):
        asyncio.run(server.call_tool("query_sql", {"sql": f"COPY (SELECT 1) TO '{target}'", "max_rows": 1}))

    assert not target.exists()
    module._reset_connection_cache()


def test_query_sql_still_runs_a_select(monkeypatch):
    module = mcp_server
    module._reset_connection_cache()
    _make_local_connection(monkeypatch, module)
    server = module.build_server()

    result = asyncio.run(server.call_tool("query_sql", {"sql": "SELECT docket_count FROM agency_stats", "max_rows": 1}))

    assert "read-only" not in str(result)
    module._reset_connection_cache()


def test_explain_analyze_cannot_modify_shared_tables(monkeypatch):
    con = duckdb.connect()
    con.execute('CREATE TABLE harmless_fixture AS SELECT 1 AS id')
    mcp_server._apply_security_settings(con)
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    with pytest.raises(ToolError, match="read-only.*EXPLAIN"):
        _tool_data(mcp_server.build_server(), "query_sql", {"sql": "EXPLAIN ANALYZE DELETE FROM harmless_fixture"})
    assert con.execute('SELECT * FROM harmless_fixture').fetchall() == [(1,)]
    con.close()


def test_catalog_configuration_is_refused_before_external_setup(monkeypatch):
    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    monkeypatch.setattr(mcp_server, "_resolve_catalog_config", lambda: {"namespace": "default"})
    monkeypatch.setattr(
        mcp_server.duckdb, "connect", lambda: (_ for _ in ()).throw(AssertionError("Database setup started"))
    )
    from spicy_regs.sources import publication

    monkeypatch.setattr(
        publication, "load_index", lambda url: (_ for _ in ()).throw(AssertionError("Remote index read"))
    )
    with pytest.raises(RuntimeError, match="MCP catalog reads require dynamic file access"):
        mcp_server._build_connection()


@pytest.mark.integration
def test_connect_queries_r2_end_to_end():
    """Live: the real ``_build_connection`` (httpfs + R2 views) serves the MCP tools.

    Covers the full path the hermetic tests cannot — httpfs install, the R2
    parquet views, and a spilling aggregation over real data — asserting none
    of it raises. Needs outbound network; run via ``pytest -m integration``.
    """
    server = mcp_server.build_server()

    assert mcp_server.TABLES[0] in {entry["table"] for entry in _listed(server)}

    schema = asyncio.run(server.call_tool("describe_table", {"table": "agency_stats"}))
    assert "column" in str(schema).lower()

    # An ORDER BY over a full remote table is the spill-prone shape that
    # crashed in production (sorts everything before applying the limit);
    # ``ORDER BY 1`` keeps it schema-agnostic. Assert it returns without error.
    result = asyncio.run(
        server.call_tool(
            "query_sql",
            {"sql": "SELECT * FROM agency_stats ORDER BY 1", "max_rows": 5},
        )
    )
    assert result is not None


# --- connection caching ------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_connection_cache():
    """Keep the module-level connection cache from leaking across tests."""
    mcp_server._reset_connection_cache()
    yield
    mcp_server._reset_connection_cache()


@dataclass
class _Publisher:
    """The live pointers a test serves, a failure to inject, and how often each seam ran."""

    publication: object
    fail: Exception | None = None
    builds: int = 0
    reads: int = 0


def _make_local_connection(monkeypatch, module) -> _Publisher:
    """Stand in a bare in-memory DuckDB for ``_build_connection`` and a settable publisher for the live pointers.

    Keeps these tests hermetic: no httpfs, no R2. A build pins the pointers and
    release configuration, as the real one does, so a refresh compares like
    with like. Set ``publication`` to move the publisher, or to an exception
    to fail its read; set ``fail`` to fail builds.
    """
    state = _Publisher(module._Publication({"families": {}, "run": 1}, None))

    def read():
        state.reads += 1
        if isinstance(state.publication, Exception):
            raise state.publication
        return state.publication

    def build(publication=None):
        state.builds += 1
        if state.fail is not None:
            raise state.fail
        con = duckdb.connect()
        con.execute("CREATE TABLE agency_stats AS SELECT 1 AS docket_count")
        con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps((publication or read()).index)])
        module._install_relationship_views(con)
        return con

    monkeypatch.setattr(module, "_build_connection", build)
    monkeypatch.setattr(module, "_read_publication", read)
    return state


def _moved(module):
    """Pointers that differ from the fixture's first publication, as after a publisher's run."""
    return module._Publication({"families": {}, "run": 2}, None)


def test_get_connection_reuses_within_ttl(monkeypatch):
    state = _make_local_connection(monkeypatch, mcp_server)

    assert mcp_server._get_connection() is mcp_server._get_connection()
    assert (state.builds, state.reads) == (1, 1)


def test_an_unmoved_publication_keeps_the_connection_past_the_ttl(monkeypatch):
    """A remote connection pins immutable URLs, so an expired one is polled, not rebuilt."""
    state = _make_local_connection(monkeypatch, mcp_server)
    first = mcp_server._get_connection()
    captured = mcp_server._pinned_record(first, "_spicy_fec_release")
    assert captured is not None
    assert set(captured["views"]) == {spec.view.name for spec in mcp_server.FEC_QUALIFIED_VIEWS}
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)

    assert mcp_server._get_connection() is first
    assert (state.builds, state.reads) == (1, 2)


def test_a_moved_publication_rebuilds_past_the_ttl_from_the_pointers_it_read(monkeypatch):
    state = _make_local_connection(monkeypatch, mcp_server)
    first = mcp_server._get_connection()
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)
    state.publication = _moved(mcp_server)

    second = mcp_server._get_connection()
    assert second is not first
    assert (state.builds, state.reads) == (2, 2)  # the build pins the pointers the poll read
    assert mcp_server._pinned_publication(second) == state.publication


@pytest.mark.parametrize("failing", ["pointer read", "build"])
def test_a_failed_refresh_keeps_serving_the_pinned_connection(monkeypatch, failing):
    """A 429 on a pointer or a member must not fail the call that happened to trigger the refresh."""
    state = _make_local_connection(monkeypatch, mcp_server)
    first = mcp_server._get_connection()
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)
    error = RuntimeError("HTTP 429 Too Many Requests")
    if failing == "build":
        state.publication, state.fail = _moved(mcp_server), error
    else:
        state.publication = error

    assert mcp_server._get_connection() is first
    assert first.execute("SELECT docket_count FROM agency_stats").fetchone() == (1,)
    state.publication, state.fail = _moved(mcp_server), None
    assert mcp_server._get_connection() is not first  # the next TTL tries again


def test_a_cold_start_that_cannot_build_raises(monkeypatch):
    state = _make_local_connection(monkeypatch, mcp_server)
    state.fail = RuntimeError("Published generation member unavailable: dockets")

    with pytest.raises(RuntimeError, match="member unavailable"):
        mcp_server._get_connection()
    state.fail = None
    assert mcp_server._get_connection() is not None


def test_other_callers_keep_the_connection_while_one_rebuilds(monkeypatch):
    """A 43 s rebuild blocked every tool call behind the connection lock; now only its own caller waits."""
    import threading

    state = _make_local_connection(monkeypatch, mcp_server)
    first = mcp_server._get_connection()
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)
    state.publication = _moved(mcp_server)
    building, release = threading.Event(), threading.Event()
    build = mcp_server._build_connection

    def slow_build(publication=None):
        building.set()
        assert release.wait(10)
        return build(publication)

    monkeypatch.setattr(mcp_server, "_build_connection", slow_build)
    refreshed = []
    refresher = threading.Thread(target=lambda: refreshed.append(mcp_server._get_connection()))
    refresher.start()
    assert building.wait(10)
    assert mcp_server._get_connection() is first  # served while the rebuild is in flight
    release.set()
    refresher.join(10)
    assert refreshed and refreshed[0] is not first and mcp_server._get_connection() is refreshed[0]


def test_cursor_from_cached_connection_survives_rebuild(monkeypatch):
    """A cursor taken before a rebuild keeps serving its query: the swap drops the module's reference, never closes."""
    state = _make_local_connection(monkeypatch, mcp_server)
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)

    first = mcp_server._get_connection()
    old_cursor = first.cursor()
    state.publication = _moved(mcp_server)
    assert mcp_server._get_connection() is not first

    assert old_cursor.execute("SELECT docket_count FROM agency_stats").fetchone() == (1,)


def test_query_sql_reuses_one_connection_across_calls(monkeypatch):
    """Two ``query_sql`` calls build the backing connection once."""
    state = _make_local_connection(monkeypatch, mcp_server)
    server = mcp_server.build_server()

    for _ in range(2):
        asyncio.run(server.call_tool("query_sql", {"sql": "SELECT docket_count FROM agency_stats", "max_rows": 1}))

    assert state.builds == 1


# --- memory-limit / spill env gating (Cloud Run) -----------------------------


def test_resolve_memory_limit_unset_is_none(monkeypatch):
    module = mcp_server
    monkeypatch.delenv("SPICY_REGS_MEMORY_LIMIT", raising=False)
    assert module._resolve_memory_limit() is None


@pytest.mark.parametrize("value", ["12GB", "2048MB", "16GiB", "1.5 GB"])
def test_resolve_memory_limit_valid(value, monkeypatch):
    module = mcp_server
    monkeypatch.setenv("SPICY_REGS_MEMORY_LIMIT", value)
    assert module._resolve_memory_limit() == value


# DuckDB refuses a percentage at SET and reads a bare number as bytes.
@pytest.mark.parametrize("value", ["12GB'; SET x=1", "lots", "'", "75%", "4"])
def test_resolve_memory_limit_rejects_junk(value, monkeypatch):
    module = mcp_server
    monkeypatch.setenv("SPICY_REGS_MEMORY_LIMIT", value)
    with pytest.raises(RuntimeError, match="valid size"):
        module._resolve_memory_limit()


def test_resolve_temp_dir_default_empty(monkeypatch):
    """Unset => '' => spilling disabled (the safe serverless default)."""
    module = mcp_server
    monkeypatch.delenv("SPICY_REGS_TEMP_DIR", raising=False)
    assert module._resolve_temp_dir() == ""


def test_resolve_temp_dir_rejects_injection(monkeypatch):
    module = mcp_server
    monkeypatch.setenv("SPICY_REGS_TEMP_DIR", "/tmp'; SET memory_limit='1kB")
    with pytest.raises(RuntimeError, match="illegal characters"):
        module._resolve_temp_dir()


def _setting(con: duckdb.DuckDBPyConnection, name: str) -> str:
    """Read the current value of a DuckDB setting."""
    row = con.execute(f"SELECT current_setting('{name}')").fetchone()
    assert row is not None
    return row[0]


def test_security_settings_default_disables_spill(monkeypatch):
    """With no env set, temp_directory stays '' — byte-identical to the prior default."""
    monkeypatch.setattr(mcp_server, "MEMORY_LIMIT", None)
    monkeypatch.setattr(mcp_server, "TEMP_DIR", "")
    con = duckdb.connect()
    mcp_server._apply_security_settings(con)
    assert _setting(con, "temp_directory") == ""


def test_security_settings_honor_memory_and_temp(tmp_path, monkeypatch):
    """When the env-derived module constants are set, the pragmas reflect them.

    This is the contract of the change — the actual spill/OOM behavior at a given
    limit is DuckDB's, and is exercised manually against the built container
    rather than pinned to a version-specific memory threshold here.
    """
    monkeypatch.setattr(mcp_server, "MEMORY_LIMIT", "1GB")
    monkeypatch.setattr(mcp_server, "TEMP_DIR", str(tmp_path))
    con = duckdb.connect()
    mcp_server._apply_security_settings(con)
    assert _setting(con, "temp_directory") == str(tmp_path)
    assert _setting(con, "memory_limit") not in ("", None)


def test_landing_page_renders_every_table():
    """The view list is substituted from TABLES, not hand-maintained in the HTML.

    The restored page shipped a hardcoded list that had drifted seven tables
    behind; this pins the substitution so it cannot silently rot again.
    """
    html = mcp_server._landing_page().decode("utf-8")
    assert "<!--VIEWS-->" not in html
    for table in mcp_server.TABLES:
        assert f'<code class="inline">{table}</code>' in html


def test_landing_assets_exist_in_package():
    """Both assets must ship inside the installed package — the container
    installs the wheel and has no repo checkout to read them from."""
    assert (mcp_server.STATIC_DIR / "index.html").is_file()
    assert mcp_server._landing_icon()[:8] == b"\x89PNG\r\n\x1a\n"


def test_app_serves_landing_page_and_mcp_endpoint():
    """/ serves the setup page and /mcp still speaks the protocol."""
    from starlette.testclient import TestClient

    with TestClient(mcp_server.build_app()) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert page.headers["content-type"].startswith("text/html")
        assert b"Spicy Regs MCP Server" in page.content

        icon = client.get("/icon.png")
        assert icon.status_code == 200
        assert icon.headers["content-type"] == "image/png"

        handshake = client.post(
            "/mcp",
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1"},
                },
            },
        )
        assert handshake.status_code == 200
        assert b'"spicy-regs"' in handshake.content
