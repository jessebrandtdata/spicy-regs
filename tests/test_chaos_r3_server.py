"""Round-3 chaos repairs: every view column described by lineage, pinned rows at discovery, bounded replies.

Evidence: corpora/mcp-chaos-2026-10-02/round3/phase2-server.md (S1, S2b, S3a, S3b) and phase3-review.md.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import duckdb
import pytest

from spicy_regs import mcp_server as server
from spicy_regs.relationship_views import RELATIONSHIP_VIEWS, SQL_RELATIONSHIP_VIEWS
from spicy_regs.relationship_views import comments as comment_views
from spicy_regs.fec_receipt_adapter import qualified_views, processing_declarations
from spicy_regs.relationship_views.lineage import Columns, column_lineage, table_columns
from spicy_regs.relationship_views.sql_views import _COLUMN_DESCRIPTIONS, install_sql_views, view_columns
from tests.test_mcp_server import _listed, _records, _tool_data

FALLBACK = "described by this view"
SOURCE = "sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0"


def _typed_tables(con, names, *, processing=False):
    """Empty tables with the dictionary's declared columns and types, so ``alias.*`` passthroughs appear."""
    for name in names:
        if processing and name in processing_declarations():
            columns = [{"column_name": n, "column_type": t} for n, t in processing_declarations()[name]["columns"]]
        else:
            columns = server._table_metadata()[name]["columns"]
        con.execute(f'CREATE TABLE "{name}" (' + ", ".join(f'"{c["column_name"]}" {c["column_type"]}' for c in columns) + ")")


def _dictionary(table: str) -> dict[str, str | None]:
    return {**processing_declarations().get(table, {}).get("descriptions", {}),
            **{c["column_name"]: c.get("description") for c in server._table_metadata().get(table, {}).get("columns", [])}}


def _dependencies() -> dict[str, list[str]]:
    deps = {}
    for spec in RELATIONSHIP_VIEWS:
        for name in spec.names:
            deps[name] = [spec.source_table]
    for name in comment_views.NAMES:
        deps[name] = ["comments"]
    for spec in SQL_RELATIONSHIP_VIEWS:
        deps[spec.name] = list(spec.required)
    return deps


def _fec_specs():
    return qualified_views(dict(source_generation_pin=SOURCE, population="fixture", as_of="fixture", namespace_evidence={}))


def _described(columns, metadata) -> list[dict]:
    return view_columns(columns, metadata.get("column_descriptions"), server._lineage_meanings(metadata.get("column_lineage", {})))


def _check_meanings(name: str, columns: list[dict], metadata: dict, declared: dict[str, str]) -> list[str]:
    """Every column's meaning is declared, the registry's, or its lineage origin's dictionary text; else None."""
    lineage = metadata.get("column_lineage", {})
    faults = []
    for column in columns:
        col, text = column["column_name"], column["description"]
        if text and FALLBACK in text:
            faults.append(f"{name}.{col}: fallback sentence")
        elif col in declared:
            if text != declared[col]:
                faults.append(f"{name}.{col}: declared meaning not used")
        elif col in _COLUMN_DESCRIPTIONS:
            if text != _COLUMN_DESCRIPTIONS[col]:
                faults.append(f"{name}.{col}: registry meaning not used")
        elif col in lineage:
            table, source = lineage[col]
            if text != _dictionary(table)[source]:
                faults.append(f"{name}.{col}: projected from {table}.{source} but not its meaning")
        elif text is not None:
            faults.append(f"{name}.{col}: computed column inherited {text[:40]!r}")
    return faults


@pytest.fixture
def registry_server(monkeypatch):
    """Every registry view bound over dictionary-typed empty tables, served through the real tools."""
    deps = _dependencies()
    with duckdb.connect() as con:
        _typed_tables(con, sorted({table for tables in deps.values() for table in tables}), processing=True)
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        yield server.build_server(), con, deps


def test_every_registry_view_column_has_a_real_description(registry_server):
    mcp, con, _ = registry_server
    relationships = server._connection_relationships(con.cursor())
    undescribed = {}
    for name, info in relationships.items():
        if info["status"] != "available":
            continue
        columns = _tool_data(mcp, "describe_table", {"table": name})["columns"]
        bad = [c["column_name"] for c in columns if not c["description"] or FALLBACK in c["description"]]
        if bad:
            undescribed[name] = bad
    assert undescribed == {}


def test_every_registry_view_meaning_is_declared_registry_or_lineage_never_a_namesake(registry_server):
    mcp, con, _ = registry_server
    declared_by = {spec.name: dict(spec.column_descriptions) for spec in SQL_RELATIONSHIP_VIEWS}
    faults = []
    for name, info in server._connection_relationships(con.cursor()).items():
        if info["status"] != "available":
            continue
        described = _tool_data(mcp, "describe_table", {"table": name})
        declared = info["metadata"].get("column_descriptions", {}) or declared_by.get(name, {})
        faults += _check_meanings(name, described["columns"], info["metadata"], declared)
        assert "column_lineage" not in described["metadata"] and "column_descriptions" not in described["metadata"]
    assert faults == []


def test_every_fec_qualified_view_column_has_a_real_description_and_honest_lineage():
    specs = _fec_specs()
    con: Any = duckdb.connect()
    _typed_tables(con, sorted({table for spec in specs for table in spec.view.required}), processing=True)
    installed = install_sql_views(con, list(processing_declarations()), [s.view for s in specs])
    undescribed, faults = {}, []
    for spec in specs:
        entry = installed[spec.view.name]
        assert entry["status"] == "available", entry
        columns = _described(con.execute(f'DESCRIBE "{spec.view.name}"').fetchall(), entry["metadata"])
        bad = [c["column_name"] for c in columns if not c["description"] or FALLBACK in c["description"]]
        if bad:
            undescribed[spec.view.name] = bad
        faults += _check_meanings(spec.view.name, columns, entry["metadata"], dict(spec.view.column_descriptions))
    assert specs and undescribed == {} and faults == []
    # Namesakes the SQL computes carry no inherited meaning: a min() over a policy table, a constant, an EXCLUDE'd recomputation.
    inclusion = installed["fec_individual_snapshot_inclusion"]["metadata"]["column_lineage"]
    assert "purpose" not in inclusion and "policy_version" not in inclusion
    assert inclusion["target_record_id"] == ["fec_receipts", "record_id"]
    resolution = installed["fec_filing_reference_resolution"]["metadata"]["column_lineage"]
    assert "target_resolution_status" not in resolution and resolution["filing_key"] == ["fec_filing_links", "filing_key"]


def test_a_projected_column_inherits_its_source_meaning_and_a_computed_namesake_does_not(registry_server):
    mcp, con, _ = registry_server
    relationships = server._connection_relationships(con.cursor())

    def meanings(name):
        return {c["column_name"]: c["description"] for c in _tool_data(mcp, "describe_table", {"table": name})["columns"]}

    candidates, links = meanings("org_identity_candidates"), _dictionary("org_committee_links")
    assert candidates["organization"] == links["organization"]
    # The match grade is a column of the link table, so the view projects it with its meaning. The names the matcher
    # compared stay in the receipt, and the summary no longer says so: that receipt holds no link and no column a
    # tester queried, and a table points at its receipt only for something a reader wants (owner decision, 2026-10-05).
    assert candidates["confidence"] == links["confidence"] and "precision bar" in (links["confidence"] or "")
    assert "organization_norm" not in candidates and "organization_norm" not in links
    summary = relationships["org_identity_candidates"]["metadata"]["summary"]
    assert "columns of the source row" in summary and "receipt" not in summary
    assert candidates["decision"].startswith("Always pending")
    parties, terms, affiliations = meanings("member_vote_party_affiliations"), _dictionary("member_vote_terms"), _dictionary("member_party_affiliations")
    assert parties["bioguide_id"] == terms["bioguide_id"] != affiliations["bioguide_id"]
    assert parties["term_index"] == terms["term_index"] != affiliations["term_index"]
    assert "unknown" in parties["party_status"] and "member_votes.party" in relationships["member_vote_party_affiliations"]["metadata"]["summary"]
    # fec_source_records also has collection_id; the view computes its own from a locator, so it must not inherit.
    # FEC processing tables are private in serving; test SQL lineage on the explicit processing schema.
    from spicy_regs.relationship_views.fec import FEC_VIEWS
    bound = install_sql_views(con, processing_declarations(), FEC_VIEWS)["fec_relationship_evidence"]
    evidence = {column["column_name"]: column["description"] for column in _described(
        con.execute("DESCRIBE fec_relationship_evidence").fetchall(), bound["metadata"])}
    assert evidence["collection_id"] != _dictionary("fec_source_records")["collection_id"]
    assert "locator" in evidence["collection_id"]
    assert evidence["source_sha256"] == _dictionary("fec_relationships")["source_sha256"]
    # A UNION of two sources agrees on no origin; the spec says what the column is.
    assert "source namespace" in meanings("uei_identifiers")["uei"]
    assert "unified_agenda" not in relationships["uei_identifiers"]["metadata"].get("column_lineage", {})


def test_lineage_of_array_views_matches_their_templates_structurally(registry_server):
    _, con, _ = registry_server
    relationships = server._connection_relationships(con.cursor())
    from spicy_regs.relationship_views.congress import NATIVE_CONGRESS_RELATIONSHIPS, NATIVE_COMMUNICATION_RINS
    from spicy_regs.relationship_views.artifacts_topics import NATIVE_BILL_SUBJECTS
    native = {spec.name: spec for spec in (*NATIVE_CONGRESS_RELATIONSHIPS, NATIVE_COMMUNICATION_RINS, NATIVE_BILL_SUBJECTS)}
    for original in RELATIONSHIP_VIEWS:
        spec = native.get(original.name, original)
        if spec.names[0] not in relationships or relationships[spec.names[0]]["status"] != "available":
            continue
        passthrough = [*spec.source_keys, *spec.context_columns]
        expected = {
            spec.names[0]: {c: [spec.source_table, c] for c in passthrough},
            spec.names[1]: {c: [spec.source_table, c] for c in spec.source_keys},
            # the field-state view projects the held field itself under the registry's raw_field_value name
            spec.names[2]: {**{c: [spec.source_table, c] for c in passthrough},
                            **({spec.detail_read_column: [spec.source_table, spec.detail_read_column]}
                               if spec.detail_read_column else {}),
                            "raw_field_value": [spec.source_table, spec.source_field]},
        }
        for name, lineage in expected.items():
            if name in relationships and relationships[name]["metadata"].get("rule_version") != "native-subject-navigation/1":
                assert relationships[name]["metadata"].get("column_lineage", {}) == lineage, name


def test_lineage_reads_the_parse_tree_not_the_column_names():
    con: Any = duckdb.connect()
    con.execute("CREATE TABLE a (k VARCHAR, j VARCHAR, record_id VARCHAR, ms STRUCT(native_field VARCHAR, value DECIMAL(38,9))[])")
    con.execute("CREATE TABLE b (k VARCHAR, z VARCHAR)")
    relations = {"a": table_columns("a", ["k", "j", "record_id", "ms"]), "b": table_columns("b", ["k", "z"])}
    cases = {
        # a UNION keeps a position's origin only when both branches agree; aliases count as projections
        "SELECT s.*, 'x' AS r, coalesce(t.n,0) AS c FROM a s LEFT JOIN (SELECT k, count(*) AS n FROM b GROUP BY k) t ON s.k=t.k "
        "UNION ALL SELECT s.*, 'y', 1 FROM a s": {"k": ["a", "k"], "j": ["a", "j"], "record_id": ["a", "record_id"], "ms": ["a", "ms"]},
        "SELECT k FROM a UNION ALL SELECT k FROM b": {},
        # a CTE star carries origins; its computed column (a namesake of b.z) carries none
        "WITH o AS (SELECT *, json_extract_string(j,'$.z') AS z FROM a) SELECT r.*, 2 AS q FROM o r": {"k": ["a", "k"], "j": ["a", "j"], "record_id": ["a", "record_id"], "ms": ["a", "ms"]},
        "SELECT v.*, m.n FROM a v LEFT JOIN LATERAL (SELECT count(*) AS n FROM b x WHERE x.k=v.k) m ON TRUE": {"k": ["a", "k"], "j": ["a", "j"], "record_id": ["a", "record_id"], "ms": ["a", "ms"]},
        "SELECT o.* EXCLUDE(k), 1 AS k FROM a o": {"j": ["a", "j"], "record_id": ["a", "record_id"], "ms": ["a", "ms"]},
        "SELECT s.k, CAST(e.key AS BIGINT) AS ord FROM a s, json_each(s.j) e": {"k": ["a", "k"]},
        "WITH routes(ns, m) AS (VALUES ('a','b')) SELECT t.k, r.m FROM a t LEFT JOIN routes r ON t.k=r.ns": {"k": ["a", "k"]},
        "SELECT item.measure.native_field AS n, record_id AS summary_record_id FROM a, UNNEST(ms) AS item(measure)": {"summary_record_id": ["a", "record_id"]},
        "SELECT c.k, f.sf FROM (SELECT k FROM a) c CROSS JOIN (VALUES ('x','y')) AS f(sf, tk)": {"k": ["a", "k"]},
        "SELECT min(k) AS k, z FROM b GROUP BY z": {"z": ["b", "z"]},
        # Recursive origins must hold through every iteration, not just the seed.
        "WITH RECURSIVE n(x,y) AS (SELECT k,j FROM a UNION ALL SELECT x,y FROM n WHERE false) SELECT * FROM n": {"x": ["a", "k"], "y": ["a", "j"]},
        "WITH RECURSIVE n(x,y,z) AS (SELECT k,k,k FROM a UNION ALL SELECT y,z,upper(z) FROM n WHERE false) SELECT * FROM n": {},
        "WITH RECURSIVE n(x,y) AS (SELECT k,j FROM a UNION ALL SELECT y,x FROM n WHERE false) SELECT * FROM n": {},
        # an unqualified name beside a relation of unknown shape resolves to nothing
        "SELECT k FROM a, json_each(j) e": {},
    }
    for sql, expected in cases.items():
        assert column_lineage(con, sql, relations) == expected, sql
    con.execute("CREATE VIEW v1 AS SELECT k, 1 AS c FROM a")
    v1: Columns = [("k", ("a", "k")), ("c", None)]
    assert column_lineage(con, "SELECT DISTINCT k, c FROM v1", {**relations, "v1": v1}) == {"k": ["a", "k"]}



@pytest.mark.parametrize("sql", [
    "WITH n(x) AS (SELECT k,j FROM a) SELECT * FROM n",
    "WITH RECURSIVE n(x) AS (SELECT k,j FROM a UNION ALL SELECT x,j FROM n WHERE false) SELECT * FROM n",
    "WITH RECURSIVE n(x) AS (SELECT k,j FROM a UNION ALL SELECT p.y,p.j FROM n AS p(y) WHERE false) SELECT * FROM n",
    "SELECT * FROM a AS n(x)",
    "SELECT * FROM (SELECT k,j FROM a) AS n(x)",
])
def test_partial_aliases_preserve_remaining_column_lineage(sql):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE a(k VARCHAR, j VARCHAR)")
        # Bind with DuckDB too: parsing alone cannot establish valid alias shape.
        result = con.execute(sql)
        assert [column[0] for column in result.description] == ["x", "j"]
        assert column_lineage(con, sql, {"a": table_columns("a", ["k", "j"])}) == {
            "x": ["a", "k"], "j": ["a", "j"]}


def test_array_detail_columns_carry_their_declared_meaning(registry_server):
    mcp, _, _ = registry_server
    columns = {c["column_name"]: c["description"] for c in _tool_data(mcp, "describe_table", {"table": "house_communication_rins_occurrences"})["columns"]}
    assert {"extraction_rule", "span_start", "matched_text"}.isdisjoint(columns)
    assert "reference" in columns["target_key"].lower()
    assert "position" in columns["source_ordinal"].lower()


def test_an_undeclared_column_is_reported_as_undescribed_not_paraphrased():
    assert view_columns([("mystery", "VARCHAR")]) == [{"column_name": "mystery", "column_type": "VARCHAR", "description": None}]
    [column] = view_columns([("uei", "VARCHAR")], {"uei": "spec"}, {"uei": "dictionary"})
    assert column["description"] == "spec"
    [column] = view_columns([("rule_version", "VARCHAR")], None, {"rule_version": "dictionary"})
    assert column["description"] == _COLUMN_DESCRIPTIONS["rule_version"]


def _index(rows: dict[str, int]) -> dict:
    return {"families": {"bill-family": {
        "artifactDigest": "sha256:" + "b" * 64, "prefix": "generations/bill-family/" + "b" * 64,
        "tables": {f"{name}.parquet": {"rows": count, "sha256": "sha256:" + "c" * 64,
                                       "columns": [[c["column_name"], c["column_type"]] for c in server._table_metadata()[name]["columns"]]}
                   for name, count in rows.items()},
    }}}


@pytest.fixture
def pinned_server(monkeypatch):
    """A publication index pinning two bill-family tables, one of them empty, beside an unpinned legacy table."""
    monkeypatch.setattr(server, "R2_BASE_URL", server._ledger()[0]["destination"])
    with duckdb.connect() as con:
        _typed_tables(con, ["financial_changes", "bill_versions", "comments"])
        con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(_index({"financial_changes": 0, "bill_versions": 225_893}))])
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        yield server.build_server(), con


def test_list_sources_states_each_pinned_tables_rows_so_an_empty_generation_is_visible_at_discovery(pinned_server):
    mcp, _ = pinned_server
    tables = {entry["table"]: entry for entry in _listed(mcp)}
    assert tables["financial_changes"]["rows"] == 0
    assert tables["bill_versions"]["rows"] == 225_893
    assert tables["comments"]["rows"] is None  # a legacy table no pointer pins
    assert set(tables["financial_changes"]) == {"table", "label", "coverage", "rows"}
    assert _tool_data(mcp, "describe_table", {"table": "financial_changes"})["publication"]["rows"] == 0


def test_describe_omits_only_measurements_and_ledger_statements_by_default_and_says_so(pinned_server):
    mcp, _ = pinned_server
    compact = _tool_data(mcp, "describe_table", {"table": "bill_versions"})
    full = _tool_data(mcp, "describe_table", {"table": "bill_versions", "detail": True})
    joins = compact["joins"]["outgoing"] + compact["joins"]["incoming"]
    assert joins and all("measurement" not in join for join in joins)
    assert all({"kind", "baseline_keys", "baseline_missing"} <= set(join) for join in joins)
    # Incoming compact groups factor shared parent fields and omit empty reasons/null floors.
    # A missing floor means unmeasured, never a zero-percent floor; populated values stay exact.
    assert all(join.get("floor_pct") is None or 0 <= join["floor_pct"] <= 100 for join in joins)
    declared = server._joins()["joins"]
    for direction in ("outgoing", "incoming"):
        key = "child" if direction == "outgoing" else "parent"
        original = [join for join in declared if join[key] == "bill_versions"]
        for observed, canonical in zip(compact["joins"][direction], original, strict=True):
            for field in ("kind", "baseline_keys", "baseline_missing", "child", "child_columns"):
                assert observed[field] == canonical[field]
            assert observed.get("reason", "") == canonical["reason"]
            assert observed.get("floor_pct") == canonical["floor_pct"]
            assert observed.get("parent", compact["joins"]["incoming_parent"]) == canonical["parent"]
            assert observed.get("parent_columns", compact["joins"].get("incoming_parent_columns")) == canonical["parent_columns"]
    assert "ledger_statements" not in compact["qualification"]
    assert {"status", "generation", "live_pin", "ledger_pin", "ledger_disposition", "ledger_tasks"} <= set(compact["qualification"])
    # Since its /1 history (ss-aj7t) the columns bill_versions /2 added are a third key only the detailed reply carries.
    assert compact["detail"] == {"full": False, "omitted": ["joins[].measurement", "qualification.ledger_statements",
                                                            "metadata.later_policy_columns"]}
    assert full["metadata"]["later_policy_columns"] == ["publication_dates", "publication_evidence_status"]
    assert full["detail"] == {"full": True, "omitted": []}
    expected = server._table_joins("bill_versions", measurements=True)
    assert full["joins"] == expected
    # Measurements for changed native keys were invalidated; detail must not revive old evidence.
    assert not any(join.get("measurement") for join in expected["outgoing"] + expected["incoming"])
    assert full["qualification"]["ledger_statements"]
    # The compact reply is a projection of the full one: every value it carries is the full reply's value.
    for key, value in compact.items():
        if key == "detail":
            continue
        if key == "joins":
            for direction in ("outgoing", "incoming"):
                for cut, whole in zip(value[direction], full["joins"][direction], strict=True):
                    assert cut == {k: v for k, v in whole.items() if k != "measurement"}
        elif key == "qualification":
            assert value == {k: v for k, v in full["qualification"].items() if k != "ledger_statements"}
        elif key == "metadata":
            assert value == {k: v for k, v in full["metadata"].items() if k != "later_policy_columns"}
        else:
            assert value == full[key]
    compact_bytes = len(json.dumps(compact, separators=(",", ":")).encode())
    full_bytes = len(json.dumps(full, separators=(",", ":")).encode())
    # Detailed replies also avoid repeating incoming evidence; a relative 2x
    # ratio would penalize that reduction. The catalog budget test caps both.
    assert compact_bytes < full_bytes, (compact_bytes, full_bytes)


@pytest.fixture
def wide_cells(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE notes (id INTEGER, body VARCHAR, tags VARCHAR[])")
        con.execute("INSERT INTO notes VALUES (0, repeat('x', 10240), ['a', 'b']), (1, 'short', ['c']), (2, NULL, NULL)")
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        yield server.build_server()


def test_max_cell_chars_cuts_long_cells_and_lists_each_cut_with_its_full_length(wide_cells):
    reply = _tool_data(wide_cells, "query_sql", {"sql": "SELECT * FROM notes ORDER BY id", "max_rows": 3, "max_cell_chars": 256})
    assert reply["truncated"] is False and reply["row_count_shown"] == 3 and reply["max_cell_chars"] == 256
    assert reply["truncated_cells"] == [{"row": 0, "column": "body", "chars": 10240}]
    assert _records(reply)[0]["body"] == "x" * 256 and _records(reply)[0]["tags"] == ["a", "b"]
    assert _records(reply)[1] == {"id": 1, "body": "short", "tags": ["c"]}
    assert _records(reply)[2] == {"id": 2, "body": None, "tags": None}


def test_a_cell_of_exactly_max_cell_chars_is_not_cut_and_lists_cut_as_json_text(wide_cells):
    exact = _tool_data(wide_cells, "query_sql", {"sql": "SELECT body FROM notes WHERE id = 0", "max_cell_chars": 10240})
    assert exact["truncated_cells"] == [] and len(_records(exact)[0]["body"]) == 10240
    lists = _tool_data(wide_cells, "query_sql", {"sql": "SELECT tags FROM notes WHERE id = 0", "max_cell_chars": 4})
    assert _records(lists)[0]["tags"] == '["a"' and lists["truncated_cells"] == [{"row": 0, "column": "tags", "chars": 9}]


def test_without_max_cell_chars_nothing_is_cut(wide_cells):
    reply = _tool_data(wide_cells, "query_sql", {"sql": "SELECT body FROM notes WHERE id = 0"})
    assert reply["max_cell_chars"] is None and reply["truncated_cells"] == [] and len(_records(reply)[0]["body"]) == 10240


def test_reflected_tool_descriptions_explain_the_new_knobs():
    tools = asyncio.run(server.build_server().list_tools())
    descriptions = {tool.name: tool.description or "" for tool in tools}
    assert "rows" in descriptions["list_sources"] and "publishes no rows" in descriptions["list_sources"]
    assert "after the pin" in descriptions["list_sources"]
    assert "detail" in descriptions["describe_table"] and "measurement" in descriptions["describe_table"]
    assert all(term in descriptions["query_sql"] for term in ("max_cell_chars", "truncated_cells", "OFFSET"))
    schema = {tool.name: tool.input_schema for tool in tools}
    assert "max_cell_chars" in schema["query_sql"]["properties"] and "detail" in schema["describe_table"]["properties"]
    assert schema["query_sql"]["required"] == ["sql"] and schema["describe_table"]["required"] == ["table"]


def test_incoming_groups_preserve_meaningful_reasons_and_zero_floors(monkeypatch):
    from copy import deepcopy
    record = deepcopy(server._joins())
    incoming = [join for join in record["joins"] if join["parent"] == "bill_versions"]
    assert incoming
    incoming[0]["reason"] = "Declared scope remains meaningful."
    incoming[0]["floor_pct"] = 0.0
    monkeypatch.setattr(server, "_joins", lambda: record)
    shaped = server._table_joins("bill_versions", measurements=False)["incoming"][0]
    assert shaped["reason"] == incoming[0]["reason"]
    assert shaped["floor_pct"] == 0.0
    assert "measurement" not in shaped
