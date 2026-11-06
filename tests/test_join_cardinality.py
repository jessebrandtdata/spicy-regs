"""Full identity matching and raw-row amplification are distinct from key coverage."""
from dataclasses import replace
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.check_table_joins import measure
from spicy_regs.table_joins import JOINS, Join


@pytest.mark.parametrize("columns, child_key, parent_keys", [
    (("bill_id","version_code","source"), ("119-hr-1","ih","govinfo"),
     [("119-hr-1","ih","govinfo"),("119-hr-1","ih","congress")]),
    (("package_id","part_id"), ("CRPT-119hrpt1","1"),
     [("CRPT-119hrpt1","1"),("CRPT-119hrpt2","1")]),
    (("congress","chamber","event_id"), ("119","house","1"),
     [("119","house","1"),("118","house","1"),("119","senate","1")]),
])
def test_reused_component_does_not_amplify_full_key(tmp_path, columns, child_key, parent_keys):
    paths = {}
    for name, rows in [("child",[child_key,child_key]),("parent",parent_keys)]:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({c:[row[i] for row in rows] for i,c in enumerate(columns)}),path)
        paths[name]=[str(path)]
    join = Join("child",columns,"parent",columns,1,0,expected_cardinality="one")
    with duckdb.connect() as con:
        result=measure(con,join,paths.__getitem__)
        assert result["keys"]==1 and result["missing"]==0
        assert result["child_input_rows"]==result["inner_join_rows"]==2
        assert result["max_parent_multiplicity"]==1
        # Removing the scope/provider column recreates a many-match reference.
        short = replace(join,child_columns=columns[:-1],parent_columns=columns[:-1])
        if columns[0]=="package_id":
            short=replace(join,child_columns=("part_id",),parent_columns=("part_id",))
        elif columns[0]=="congress":
            short=replace(join,child_columns=("event_id",),parent_columns=("event_id",))
        amplified=measure(con,short,paths.__getitem__)
        assert amplified["status"]=="MULTIPLICITY"
        assert amplified["inner_join_rows"]>result["inner_join_rows"]


def test_full_selected_receipts_cover_new_keys_and_populated_attributes():
    measured=[j for j in JOINS if j.measurement]
    assert {j.child for j in measured} >= {"hearing_transcripts",
                                         "document_attributes","docket_attributes"}
    for join in measured:
        assert join.measurement is not None
        assert join.expected_cardinality=="one"
        assert join.measurement["scope"]=="full_selected_inputs"
        assert (join.baseline_keys, join.baseline_missing) == (
            join.measurement["keys"], join.measurement["missing"])
        assert 0 <= join.baseline_missing <= join.baseline_keys
        assert join.kind in {"complete", "scope", "design", "empty"}
        assert 0 <= join.measurement["max_parent_multiplicity"] <= 1
        assert join.measurement["parent_duplicate_keys"] == 0
        for side in ['child','parent']:
            urls=join.measurement[side+'_urls']
            if all('/generations/' in url for url in urls):
                continue
            selected=join.measurement['selected_inputs'][side]
            assert urls == [m['url'] for m in selected['members']]
            assert selected['publicationIdentity'].startswith('sha256:')
            if any(m.get('etag') for m in selected['members']):
                assert join.measurement['mutable_versions_checked_before_after'] is True
                assert all(m['etag'] and m['byteSize'] > 0 for m in selected['members'])
            else:
                assert all('/materialized/rulemaking/snapshots/snapshot_' in url for url in urls)


def test_dated_report_keys_keep_the_undated_measurement_as_historical_evidence():
    join = next(j for j in JOINS if j.child == "report_sections" and j.parent == "committee_reports")
    assert join.child_columns == join.parent_columns == ("package_id", "part_id", "last_modified")
    assert join.kind == "unmeasured" and join.measurement is None
    assert (join.baseline_keys, join.baseline_missing, join.floor_pct) == (0, 0, None)
    path = Path(__file__).parents[1] / "src/spicy_regs/join_measurements.json"
    historical = next(row for row in json.loads(path.read_text())["results"]
                      if row["join"] == "report_sections.package_id+part_id -> committee_reports.package_id+part_id")
    assert historical["scope"] == "full_selected_inputs"
    assert (historical["keys"], historical["missing"]) == (146, 0)
    assert historical["parent_duplicate_keys"] == 0 and historical["max_parent_multiplicity"] == 1
    assert historical["expected_cardinality"] == "one"
    assert all("/generations/" in url for side in ("child", "parent") for url in historical[side + "_urls"])


def test_changed_printing_keys_do_not_inherit_old_measurements():
    native = [j for j in JOINS if j.child in {"bill_sections", "section_diffs"} and "printing_id" in " ".join((*j.child_columns, *j.parent_columns))]
    assert native
    assert all(j.measurement is None and j.baseline_keys == 0 for j in native)


@pytest.mark.parametrize("parent", ["budget_volumes", "house_activity_reports"])
def test_dated_metadata_body_can_match_multiple_parent_revisions(tmp_path, parent):
    join = next(j for j in JOINS if j.child == "document_citations" and j.parent == parent)
    assert join.kind == "unmeasured" and join.measurement is None
    assert join.expected_cardinality == "many"
    assert (join.baseline_keys, join.baseline_missing, join.floor_pct) == (0, 0, None)
    child, parents = tmp_path / "citations.parquet", tmp_path / "parents.parquet"
    pq.write_table(pa.table({"document_key": ["pkg"], "body_version_id": ["same-body"]}), child)
    pq.write_table(pa.table({"package_id": ["pkg", "pkg"], "body_version_id": ["same-body", "same-body"],
                            "last_modified": ["2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z"]}), parents)
    urls = {join.child: [str(child)], join.parent: [str(parents)]}
    with duckdb.connect() as connection:
        actual = measure(connection, join, urls.__getitem__)
        assert actual["inner_join_rows"] == 2
        assert actual["max_parent_multiplicity"] == 2
        assert actual["status"] != "MULTIPLICITY"
        wrong = measure(connection, replace(join, expected_cardinality="one"), urls.__getitem__)
        assert wrong["status"] == "MULTIPLICITY"


def test_dated_report_section_key_has_no_undated_measurement():
    join, = [j for j in JOINS if j.child == "report_sections" and j.parent == "committee_reports"]
    assert join.child_columns == join.parent_columns == ("package_id", "part_id", "last_modified")
    assert join.kind == "unmeasured" and join.measurement is None
    assert (join.baseline_keys, join.baseline_missing) == (0, 0)
