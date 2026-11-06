"""Audited navigation uses the canonical joins and the same CI regression checks."""

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.check_table_joins import measure
from spicy_regs import data_dictionary, table_joins

ROOT = Path(__file__).parents[1]
DATA = ROOT / "src" / "spicy_regs"
AUDIT = json.loads((DATA / "join_audit.json").read_text())
RECEIPT = json.loads((ROOT / "docs/evidence/explorer-joins-2026-10-03.json").read_text())
SCORECARD_RECEIPT = json.loads((ROOT / "docs/evidence/scorecard-navigation-2026-10-03.json").read_text())
REVIEWED_NAMES = {item["measurement"]["join"] for item in [*RECEIPT["results"], *SCORECARD_RECEIPT["joins"]]}
REVIEWED = [join for join in table_joins.SOURCE_JOINS if join.name in REVIEWED_NAMES]


def key(join):
    return join["child"], tuple(join["child_columns"]), join["parent"], tuple(join["parent_columns"])


def test_audit_accounts_for_every_previously_unconnected_published_table():
    assert set(AUDIT) == set(RECEIPT["published_tables"])
    for name in RECEIPT["previous_without_usable_published_join"]:
        entry = AUDIT[name]
        assert entry["status"] in {"connected", "standalone", "missing", "ambiguous", "special-handling"}
        assert entry["reason"] and entry["evidence"]
        if entry["status"] == "connected":
            assert entry["relationships"]
    # Repeating typed FEC IDs and FCC array memberships need conditions the
    # explorer's simple scalar-equality navigation cannot express.
    for name in ("fec_relationships", "fcc_filings", "fcc_proceedings"):
        assert AUDIT[name]["status"] == "special-handling"


def test_every_audited_connection_is_in_the_single_canonical_registry():
    schemas = data_dictionary.expected_schemas()
    assert {join.name for join in REVIEWED} == REVIEWED_NAMES
    assert len(REVIEWED) == len(REVIEWED_NAMES)
    for declaration in REVIEWED:
        join = table_joins.record(declaration)
        assert len(join["child_columns"]) == len(join["parent_columns"]) > 0
        assert declaration in table_joins.JOINS or declaration in table_joins.RETIRED_PROCESSING_JOINS
    assert table_joins.declaration_errors(schemas) == []


def test_audited_measurements_supply_enforced_baselines_and_cardinality():
    for join in REVIEWED:
        measurement = join.measurement
        assert measurement is not None
        assert measurement["scope"] == "full_selected_inputs"
        assert measurement["parent_duplicate_keys"] == 0
        assert measurement["measured_expected_cardinality"] == "one"
        assert 0 <= measurement["missing"] <= measurement["keys"]
        assert join.expected_cardinality == "one"
        assert (join.baseline_keys, join.baseline_missing) == (measurement["keys"], measurement["missing"])
        assert (join.floor_pct is None) == (measurement["keys"] == 0)
        for side in ("child", "parent"):
            assert measurement[f"{side}_urls"]
            assert all("/generations/" in url for url in measurement[f"{side}_urls"])
        if measurement["keys"] == 0:
            assert join.kind == "empty"
        elif measurement["missing"]:
            assert join.kind == "scope"


@pytest.mark.parametrize("parent", ["budget_volumes", "house_activity_reports"])
def test_native_citation_navigation_preserves_dated_parent_revisions(tmp_path, parent):
    historical = next(join for join in table_joins.SOURCE_JOINS
                      if join.child == "document_citations" and join.parent == parent)
    assert historical.kind == "scope" and historical.expected_cardinality == "one"
    assert historical.measurement is not None
    assert historical.measurement["measured_expected_cardinality"] == "one"
    join = next(join for join in table_joins.JOINS
                if join.child == "document_citations" and join.parent == parent)
    assert join.kind == "unmeasured" and join.expected_cardinality == "many"
    assert join.measurement is None
    assert (join.baseline_keys, join.baseline_missing) == (0, 0)
    child_file, parent_file = tmp_path / "citations.parquet", tmp_path / "revisions.parquet"
    pq.write_table(pa.table({"document_key": ["package"], "body_version_id": ["body"]}), child_file)
    pq.write_table(pa.table({"package_id": ["package", "package"], "body_version_id": ["body", "body"],
                            "last_modified": ["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"]}), parent_file)
    urls = {join.child: [str(child_file)], join.parent: [str(parent_file)]}
    with duckdb.connect() as con:
        result = measure(con, join, urls.__getitem__)
    assert result["status"] == "UNBASELINED"
    assert result["inner_join_rows"] == 2 and result["max_parent_multiplicity"] == 2
    assert result["missing"] == 0


@pytest.mark.parametrize("declaration", [table_joins.record(j) for j in REVIEWED if len(j.child_columns) > 1], ids=key)
def test_navigation_keys_do_not_cross_edition_digest_or_member_scope(tmp_path, declaration):
    child_columns, parent_columns = declaration["child_columns"], declaration["parent_columns"]
    # A second parent agrees on the apparent ID, but not the final scope field.
    # A third child has no complete key and must not become an accidental match.
    child: dict[str, list[str | None]] = {column: ["same", "same", "same"] for column in child_columns}
    child[child_columns[-1]] = ["scope-a", "scope-missing", None]
    parent = {column: ["same", "same"] for column in parent_columns}
    parent[parent_columns[-1]] = ["scope-a", "scope-b"]
    child_file, parent_file = tmp_path / "child.parquet", tmp_path / "parent.parquet"
    pq.write_table(pa.table(child), child_file)
    pq.write_table(pa.table(parent), parent_file)
    join = table_joins.Join(declaration["child"], tuple(child_columns), declaration["parent"],
                            tuple(parent_columns), 2, 1, expected_cardinality="one")
    urls = {join.child: [str(child_file)], join.parent: [str(parent_file)]}
    with duckdb.connect() as con:
        result = measure(con, join, urls.__getitem__)
    assert (result["keys"], result["missing"], result["child_nonnull_rows"]) == (2, 1, 2)
    assert result["parent_duplicate_keys"] == 0
    assert result["inner_join_rows"] == 1
