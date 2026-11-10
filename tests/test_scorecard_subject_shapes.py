"""Domain conversion preserves publisher meaning and exact conversion inputs."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import duckdb
from spicy_docs.schemas.scorecard_tables import SCORECARD_TABLES
from spicy_regs.scorecards.resolution import LINK_COLUMNS
from spicy_regs.scorecards.action_comparisons import TABLE as COMPARISON_TABLE, COLUMNS as COMPARISON_COLUMNS, KEYS as COMPARISON_KEYS
from spicy_regs.scorecards.subject_shapes import (
    SOURCE_COLUMNS,
    DOMAIN_COLUMNS,
    map_source_row,
    restore_source_row,
    subject_schema,
)


def row(name, **values):
    result = dict.fromkeys(SOURCE_COLUMNS[name])
    from spicy_regs.scorecards.subject_shapes import IDENTITIES

    result.update({key: key for key in IDENTITIES[name]})
    return result | values


def test_every_provider_and_resolver_field_is_explicitly_classified():
    actual = {name: set(c.columns) for name, c in SCORECARD_TABLES.items()}
    actual.update({name: set(columns) for name, columns in LINK_COLUMNS.items()})
    actual[COMPARISON_TABLE] = set(COMPARISON_COLUMNS)
    assert actual == {name: set(columns) for name, columns in SOURCE_COLUMNS.items()}
    assert DOMAIN_COLUMNS["scorecard_snapshots"] == SOURCE_COLUMNS["scorecard_snapshots"]
    assert DOMAIN_COLUMNS[COMPARISON_TABLE] == COMPARISON_COLUMNS


@pytest.mark.parametrize("value", [None, "[]", '["alias","alias","last"]'])
def test_alias_order_repeats_and_null_empty_distinction(value):
    before = row("scorecard_publishers", aliases_json=value)
    mapped = map_source_row("scorecard_publishers", before)
    assert mapped["aliases"] == (json.loads(value) if value is not None else None)
    assert restore_source_row("scorecard_publishers", mapped) == before


def test_identifiers_and_occurrences_preserve_structure_and_receipt_locators():
    identifiers = '[{"scheme":"id","value":"a"},{"scheme":"id","value":"a"}]'
    mapped = map_source_row("scorecard_members", row("scorecard_members", identifiers_json=identifiers))
    assert mapped["identifiers"] == json.loads(identifiers)
    references = json.dumps(
        [
            dict(occurrence_id="a", citation_text="H.R. 1", kind="bill", source_path="p1", chamber_text=None),
            dict(occurrence_id="b", citation_text="H.R. 1", kind="bill", source_path="p2"),
        ]
    )
    before = row("scorecard_items", references_json=references)
    mapped = map_source_row("scorecard_items", before)
    assert [r["occurrence_id"] for r in mapped["references"]] == ["a", "b"]
    assert [r["citation_text"] for r in mapped["references"]] == ["H.R. 1", "H.R. 1"]
    assert all("source_path" not in r for r in mapped["references"])
    assert restore_source_row("scorecard_items", mapped) == before


@pytest.mark.parametrize("value", ["null", "{}", "[null]", '[{"scheme":"a","scheme":"b","value":"1"}]'])
def test_invalid_lists_refuse_without_coercion(value):
    with pytest.raises(ValueError):
        map_source_row("scorecard_members", row("scorecard_members", identifiers_json=value))


def test_exact_numeric_and_status_facts():
    before = row(
        "scorecard_member_ratings",
        value_text="97.123456789012345",
        value_number="+097.123456789012345",
        value_status_text="not graded",
    )
    mapped = map_source_row("scorecard_member_ratings", before)
    assert mapped["value_number"] == "+097.123456789012345"
    assert mapped["value_status_text"] == "not graded"
    assert restore_source_row("scorecard_member_ratings", mapped) == before
    assert subject_schema("scorecard_member_ratings").field("value_number").type == pa.string()
    before = row("scorecard_metric_items", counts_toward_metric="false", weight_number="-2.00")
    mapped = map_source_row("scorecard_metric_items", before)
    assert mapped["counts_toward_metric"] is False
    assert restore_source_row("scorecard_metric_items", mapped) == before
    assert "disclosure_status" in subject_schema("scorecard_methodologies").names
    assert "eligibility_text" in subject_schema("scorecard_members").names


@pytest.mark.parametrize("literal", ["0.0047169811320754715", "0.0070921985815602835", "-0.0047169811320754715"])
def test_nineteen_digit_rating_survives_arrow_duckdb_and_exact_source_replay(literal):
    before = row("scorecard_member_ratings", value_text=literal, value_number=literal)
    mapped = map_source_row("scorecard_member_ratings", before)
    table = pa.Table.from_pylist([mapped], schema=subject_schema("scorecard_member_ratings"))
    with duckdb.connect() as connection:
        connection.register("ratings", table)
        found = connection.execute("SELECT value_number FROM ratings").fetchone()
        assert found is not None
        value = found[0]
    assert value == literal
    assert restore_source_row("scorecard_member_ratings", mapped) == before


@pytest.mark.parametrize(
    "literal",
    [
        "88.888888888888885730921174399554729461669921875",
        "-0.00000000000000000000000000000000000000000000001",
        "+00088.888888888888885730921174399554729461669921875",
        "10000000000000000000",
    ],
)
def test_rating_lexeme_survives_parquet_duckdb_without_a_precision_bound(tmp_path, literal):
    before = row("scorecard_member_ratings", value_text=literal, value_number=literal)
    mapped = map_source_row("scorecard_member_ratings", before)
    path = tmp_path / "ratings.parquet"
    pq.write_table(pa.Table.from_pylist([mapped], schema=subject_schema("scorecard_member_ratings")), path)
    with duckdb.connect() as connection:
        declared = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        assert next(value[1] for value in declared if value[0] == "value_number") == "VARCHAR"
        assert connection.execute("SELECT value_number FROM read_parquet(?)", [str(path)]).fetchone() == (literal,)
    assert mapped["value_number"] == literal
    assert restore_source_row("scorecard_member_ratings", mapped) == before


@pytest.mark.parametrize("value", ["NaN", "1e2", "Infinity", "94%", " 94", "", "A+"])
def test_non_decimal_rating_text_refuses(value):
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        map_source_row("scorecard_member_ratings", row("scorecard_member_ratings", value_number=value))


def test_unknown_fields_refuse():
    with pytest.raises(ValueError, match="classify every field"):
        map_source_row("scorecards", row("scorecards", new_field="must audit"))


def test_period_variants_and_empty_component_table_have_declared_native_types():
    periods = json.dumps(
        [
            dict(
                occurrence_id="period-1",
                period_text="118th Congress",
                kind="explicit",
                source_path="p1",
                congress_text="118",
            ),
            dict(occurrence_id="period-2", period_text="Lifetime", kind="relative", source_path="p2"),
        ]
    )
    before = row("scorecards", periods_json=periods)
    mapped = map_source_row("scorecards", before)
    assert [entry["kind"] for entry in mapped["periods"]] == ["explicit", "relative"]
    assert mapped["periods"][1]["congress_text"] is None
    assert restore_source_row("scorecards", mapped) == before
    assert subject_schema("scorecard_metric_components").field("weight_number").type == pa.decimal128(38, 18)


@pytest.mark.parametrize("literal,native", [(None, None), ("true", True), ("false", False)])
def test_boolean_null_is_not_false(literal, native):
    before = row("scorecard_metrics", is_primary=literal)
    mapped = map_source_row("scorecard_metrics", before)
    assert mapped["is_primary"] is native
    assert restore_source_row("scorecard_metrics", mapped) == before


def test_resolved_legislative_numbers_are_native_and_replay_exactly():
    before = row("scorecard_item_links", congress="119", session="1", roll_number="597")
    mapped = map_source_row("scorecard_item_links", before)
    assert (mapped["congress"], mapped["session"], mapped["roll_number"]) == (119, 1, 597)
    assert restore_source_row("scorecard_item_links", mapped) == before


def test_shared_source_declarations_preserve_prior_columns_order_and_keys():
    from pathlib import Path
    from spicy_regs.scorecards.subject_shapes import SOURCE_COLUMNS, IDENTITIES
    baseline = json.loads((Path(__file__).parent / "fixtures/scorecard_source_shapes_before_refactor.json").read_text())
    assert set(SOURCE_COLUMNS) == set(baseline["SOURCE_COLUMNS"]) | {COMPARISON_TABLE}
    assert set(IDENTITIES) == set(baseline["IDENTITIES"]) | {COMPARISON_TABLE}
    additions = {"normalized_state", "state_status", "normalized_district", "district_status", "correction_evidence_json",
                 "geography_evidence_json"}
    assert {k: [c for c in SOURCE_COLUMNS[k] if k != "scorecard_member_links" or c not in additions]
            for k in baseline["SOURCE_COLUMNS"]} == baseline["SOURCE_COLUMNS"]
    assert additions <= set(SOURCE_COLUMNS["scorecard_member_links"])
    assert {k: list(IDENTITIES[k]) for k in baseline["IDENTITIES"]} == baseline["IDENTITIES"]
    assert SOURCE_COLUMNS[COMPARISON_TABLE] == COMPARISON_COLUMNS
    assert IDENTITIES[COMPARISON_TABLE] == COMPARISON_KEYS


def test_earlier_member_link_policy_and_row_do_not_invent_new_geography():
    from dataclasses import replace
    from spicy_regs.policy_migrations import earlier_policies
    from spicy_regs.scorecards.etl import POLICIES, admitted_read_policies

    current = POLICIES["scorecard_member_links"]
    geography, previous, original = earlier_policies(current)
    assert earlier_policies(geography) == (previous, original)
    assert earlier_policies(previous) == (original,)
    assert admitted_read_policies(("scorecard_member_links",), descriptors=[previous.descriptor()]) == {
        "scorecard_member_links": previous}
    before = row("scorecard_member_links", bioguide_id="B001323", resolution_status="resolved")
    for field in ("normalized_state", "state_status", "normalized_district", "district_status", "correction_evidence_json"):
        del before[field]
    assert restore_source_row("scorecard_member_links", before) == before
    with pytest.raises(KeyError):
        restore_source_row("scorecard_member_links", before | {"normalized_district": "0"})
    for policy in (current, geography, previous):
        assert earlier_policies(replace(policy, identity_fields=("scorecard_id",))) == ()
