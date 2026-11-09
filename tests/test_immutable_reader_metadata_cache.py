"""Only complete, admitted immutable SQL URL sets permit HTTP metadata reuse."""
from copy import deepcopy
import json
from typing import Any
from pathlib import Path

import duckdb
import pytest

from spicy_regs import mcp_server as server
from spicy_regs.sources import publication as pub


@pytest.fixture(autouse=True)
def remote(monkeypatch):
    monkeypatch.setattr(server, "DATA_DIR", None)
    monkeypatch.setattr(server, "R2_BASE_URL", "https://fixture.invalid")


def index(*, receipts=True):
    value = pub.empty_index()
    digest = "sha256:" + "a" * 64
    entry: dict[str, object] = {"prefix": "generations/fixture/" + "a" * 64, "artifactDigest": digest,
             "logicalId": "urn:test:fixture", "tables": {"s.parquet": {
                 "sha256": digest, "byteSize": 1, "rows": 1, "columns": [["id", "INTEGER"]]}}}
    if receipts:
        entry["etlReceipts"] = {"key": "etl_receipts.parquet", "sha256": digest, "byteSize": 1,
                                "rows": 1, "columns": [["id", "INTEGER"]], "generationId": "g",
                                "datasets": ["s"]}
    value["families"]["fixture"] = entry
    return pub.parse_index(json.dumps(value).encode())


def paths(value):
    prefix = server.R2_BASE_URL + "/" + value["families"]["fixture"]["prefix"]
    return [prefix + "/s.parquet", prefix + "/etl_receipts.parquet"]


def snapshot(*, receipt=False):
    prefix = "materialized/rulemaking/snapshots/snapshot_" + "b" * 64
    pointer = {"dataset": "rulemaking", "format_version": 2,
               "snapshot_id": "snapshot_" + "b" * 64, "manifest_key": prefix + "/manifest.json"}
    manifest: dict[str, Any] = {**pointer, "artifacts": {"snapshot_table.parquet": {
        "visibility": "public", "remote_key": prefix + "/snapshot_table.parquet",
        "sha256": "b" * 64, "bytes": 1, "rows": 1}}}
    if receipt:
        manifest["run_id"] = "run-1"
        manifest["etlReceipts"] = {"key": "etl_receipts.parquet", "generationId": "run-1",
                                   "policies": [{"dataset": "snapshot_table"}]}
        manifest["artifacts"]["etl_receipts.parquet"] = {
            "visibility": "internal", "remote_key": prefix + "/etl_receipts.parquet",
            "sha256": "c" * 64, "bytes": 1, "rows": 1}
    docs = {server.R2_BASE_URL + "/" + pub.SNAPSHOT_POINTER: json.dumps(pointer).encode(),
            server.R2_BASE_URL + "/" + pointer["manifest_key"]: json.dumps(manifest).encode()}
    return pub.load_rulemaking_snapshot(server.R2_BASE_URL, read=lambda url, **_: docs[url])


class Settings:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)


def test_complete_managed_subject_receipt_index_and_snapshot_paths_enable_both_caches():
    value, rulemaking = index(), snapshot()
    admitted = [*paths(value), server.R2_BASE_URL + "/" + rulemaking["tables"]["snapshot_table.parquet"]["remote_key"]]
    con = Settings()
    result = server._enable_immutable_metadata_cache(con, value, rulemaking, admitted)
    assert result == {**dict.fromkeys(paths(value), "managed_generation"), admitted[-1]: "rulemaking_snapshot"}
    assert con.statements == ["SET enable_http_metadata_cache=true", "SET GLOBAL parquet_metadata_cache=true"]


def test_the_snapshot_receipt_the_build_admits_is_owned_with_its_snapshot():
    """The live snapshot declares a receipt; before it was owned, every remote connection ran without the cache."""
    value, rulemaking = index(), snapshot(receipt=True)
    assert rulemaking is not None and (declared := pub.rulemaking_receipt(rulemaking)) is not None
    tables = server.R2_BASE_URL + "/" + rulemaking["tables"]["snapshot_table.parquet"]["remote_key"]
    receipt = server.R2_BASE_URL + "/" + declared["remote_key"]
    con = Settings()
    result = server._enable_immutable_metadata_cache(con, value, rulemaking, [*paths(value), tables, receipt])
    assert result is not None and result[receipt] == "rulemaking_snapshot"
    assert con.statements == ["SET enable_http_metadata_cache=true", "SET GLOBAL parquet_metadata_cache=true"]


@pytest.mark.parametrize("extra", ["/s.parquet", "/comments.parquet", "/comments_index.parquet",
                                   "/generations/unknown/" + "c" * 64 + "/s.parquet",
                                   "/materialized/rulemaking/snapshots/wrong/snapshot_table.parquet",
                                   "/source-evidence/local-control.json"])
def test_one_unowned_or_mutable_sql_path_keeps_all_defaults(extra):
    value, rulemaking = index(), snapshot()
    admitted = [*paths(value), server.R2_BASE_URL + extra]
    con = Settings()
    assert server._enable_immutable_metadata_cache(con, value, rulemaking, admitted) is None
    assert con.statements == []


@pytest.mark.parametrize("mode", ["local", "native", "empty", "local-base", "wrong-member", "wrong-prefix"])
def test_ineligible_modes_and_mismatched_members_do_not_toggle(monkeypatch, tmp_path, mode):
    value, admitted = index(), paths(index())
    if mode in {"local", "native"}:
        monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    elif mode == "empty":
        admitted = []
    elif mode == "local-base":
        monkeypatch.setattr(server, "R2_BASE_URL", str(tmp_path))
    elif mode == "wrong-member":
        admitted[-1] += ".unknown"
    else:
        value["families"]["fixture"]["prefix"] = "generations/fixture/" + "c" * 64
    con = Settings()
    assert server._enable_immutable_metadata_cache(con, value, None, admitted) is None
    assert con.statements == []


@pytest.mark.parametrize("damage", ["snapshot-id", "manifest-key", "member-path", "returned-tables", "manifest"])
def test_snapshot_namespace_and_captured_manifest_must_agree(damage):
    value, rulemaking = index(), deepcopy(snapshot())
    location = server.R2_BASE_URL + "/" + rulemaking["tables"]["snapshot_table.parquet"]["remote_key"]
    if damage == "snapshot-id":
        rulemaking["snapshot_id"] = "wrong"
    elif damage == "manifest-key":
        rulemaking["manifest_key"] = "materialized/rulemaking/another.json"
    elif damage == "member-path":
        rulemaking["manifest"]["artifacts"]["snapshot_table.parquet"]["remote_key"] = "bare.parquet"
    elif damage == "returned-tables":
        rulemaking["tables"] = {}
    else:
        del rulemaking["manifest"]
    con = Settings()
    assert server._enable_immutable_metadata_cache(con, value, rulemaking, [*paths(value), location]) is None
    assert con.statements == []


def test_required_snapshot_view_still_gets_actual_describe(tmp_path):
    from tests.test_parallel_source_admission import TrackedConnection

    with duckdb.connect(config={"threads": 1}) as raw:
        file = tmp_path / "snapshot.parquet"
        raw.execute("COPY (SELECT 1 AS id) TO ? (FORMAT PARQUET)", [str(file)])
        statements = []
        con = TrackedConnection(raw, statements.append)
        source = server._RemoteSourceView("snapshot_table", [str(file)], None, True, False)
        assert server._admit_remote_source_views(con, [source]) == [True]
        assert any(sql == 'DESCRIBE "snapshot_table"' for sql in statements)
        assert all(c.closed for c in con.children)


def test_cache_enable_occurs_after_actual_admission_and_before_binding_and_lock(tmp_path, monkeypatch):
    from tests.test_generation_mcp import connection_fixture

    value = index(receipts=False)
    file = tmp_path / "s.parquet"
    with duckdb.connect(config={"threads": 1}) as raw:
        raw.execute("COPY (SELECT 1::INTEGER AS id) TO ? (FORMAT PARQUET)", [str(file)])
    location = pub.table_members(value, "s.parquet")[0].path
    built = connection_fixture(monkeypatch, value, {location: file}, tables=("s",))
    order = []
    enable = server._enable_immutable_metadata_cache

    def checked_enable(con, captured, rulemaking, admitted, archive):
        assert con.inner.execute("DESCRIBE s").fetchall()[0][:2] == ("id", "INTEGER")
        assert admitted == [server.R2_BASE_URL + "/" + location]
        order.append("admitted-cache")
        return enable(con, captured, rulemaking, admitted, archive)

    monkeypatch.setattr(server, "_enable_immutable_metadata_cache", checked_enable)
    monkeypatch.setattr(server, "_install_relationship_views", lambda con: order.append("relationships"))
    monkeypatch.setattr(server, "_apply_security_settings", lambda *args: order.append("security"))
    con = server._build_connection()
    try:
        assert order == ["admitted-cache", "relationships", "security"]
    finally:
        con.close()
    assert built == [con]


def test_allowlist_change_after_enable_refuses_before_security(tmp_path, monkeypatch):
    from tests.test_generation_mcp import connection_fixture

    value = index(receipts=False)
    file = tmp_path / "s.parquet"
    with duckdb.connect(config={"threads": 1}) as raw:
        raw.execute("COPY (SELECT 1::INTEGER AS id) TO ? (FORMAT PARQUET)", [str(file)])
    built = connection_fixture(monkeypatch, value, {pub.table_members(value, "s.parquet")[0].path: file}, tables=("s",))
    captured_paths = []
    enable = server._enable_immutable_metadata_cache

    def capture(con, index, rulemaking, admitted, archive):
        captured_paths.append(admitted)
        return enable(con, index, rulemaking, admitted, archive)

    monkeypatch.setattr(server, "_enable_immutable_metadata_cache", capture)
    monkeypatch.setattr(server, "_install_relationship_views",
                        lambda con: captured_paths[0].append(server.R2_BASE_URL + "/comments.parquet"))
    monkeypatch.setattr(server, "_apply_security_settings", lambda *args: pytest.fail("locked changed allowlist"))
    with pytest.raises(RuntimeError, match="Immutable SQL inputs changed"):
        server._build_connection()
    assert built[0].closed


def test_real_cache_setting_reaches_request_cursor_and_new_database_starts_fresh():
    with duckdb.connect(config={"threads": 1}) as con:
        platform_row = con.execute("PRAGMA platform").fetchone()
        assert platform_row is not None
        platform = platform_row[0]
        path = Path.home() / ".duckdb/extensions" / ("v" + duckdb.__version__) / platform / "httpfs.duckdb_extension"
        if not path.is_file():
            pytest.skip("No installed local httpfs; do not download it")
        load = "LOAD '" + str(path).replace("'", "''") + "'"
        con.execute(load)
        setting_row = con.execute("SELECT current_setting('enable_http_metadata_cache')").fetchone()
        assert setting_row is not None
        before = setting_row[0]
        value = index()
        assert server._enable_immutable_metadata_cache(con, value, None, paths(value)) is not None
        con.execute("SET enable_external_access=false")
        con.execute("SET lock_configuration=true")
        child = con.cursor()
        try:
            assert child.execute("SELECT current_setting('enable_http_metadata_cache')").fetchone() == (True,)
            assert child.execute("SELECT current_setting('parquet_metadata_cache')").fetchone() == (True,)
            with pytest.raises(duckdb.Error):
                child.execute("SET enable_external_access=true")
        finally:
            child.close()
        with duckdb.connect(config={"threads": 1}) as fresh:
            fresh.execute(load)
            assert fresh.execute("SELECT current_setting('enable_http_metadata_cache')").fetchone() == (before,)
            assert fresh.execute("SELECT current_setting('parquet_metadata_cache')").fetchone() == (False,)


def test_warmed_parsed_metadata_preserves_results_and_cannot_read_unallowed_files(tmp_path):
    """Local stand-ins exercise native cache/ACL behavior, not production local-mode eligibility."""
    allowed = tmp_path / "allowed.parquet"
    unknown = tmp_path / "unknown.parquet"
    with duckdb.connect(config={"threads": 1}) as con:
        con.execute("COPY (SELECT 7::INTEGER AS id) TO ? (FORMAT PARQUET)", [str(allowed)])
        con.execute("COPY (SELECT 99::INTEGER AS id) TO ? (FORMAT PARQUET)", [str(unknown)])
        con.execute("CREATE VIEW admitted AS SELECT * FROM " + pub.parquet_scan([str(allowed)]))
        original = con.execute("DESCRIBE admitted").fetchall()
        con.execute("SET GLOBAL parquet_metadata_cache=true")
        assert con.execute("SELECT * FROM admitted").fetchall() == [(7,)]
        server._apply_security_settings(con, [str(allowed)])
        child = con.cursor()
        try:
            assert child.execute("DESCRIBE admitted").fetchall() == original
            assert child.execute("SELECT * FROM admitted").fetchall() == [(7,)]
            assert child.execute("SELECT count(*) FROM admitted").fetchall() == [(1,)]
            for sql in ["SELECT * FROM read_parquet(?)", "SELECT count(*) FROM read_parquet(?)",
                        "SELECT * FROM parquet_metadata(?)", "SELECT * FROM parquet_file_metadata(?)"]:
                with pytest.raises(duckdb.Error, match="disabled by configuration"):
                    child.execute(sql, [str(unknown)]).fetchall()
        finally:
            child.close()
