"""The government bulk writer and restore equal the row path on real-shaped lobbying data and on the edge spellings."""

from __future__ import annotations

import json
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from loguru import logger

from spicy_regs import etl_bulk, government_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.transforms import government_receipts as gr
from spicy_regs.transforms.build_lobbying_filings import _activity_rows, _lobbyist_rows, _shape
from spicy_regs.transforms.government_source_shapes import (
    BOOLEAN_FIELDS, DATE_FIELDS, IDENTITY_FIELDS, INTEGER_FIELDS, LEGACY_COLUMNS, MONEY_FIELDS, TIMESTAMP_FIELDS,
)

GENERATION = "gen-bulk"


def _filing(i: int, rng: random.Random) -> dict:
    activities = [
        {
            "general_issue_code": rng.choice(["TEC", "BUD", "SCI"]),
            "general_issue_code_display": "Display",
            "description": f"Activity {i}.{a} \"quoted\" back\\slash \U0001f600 café",
            "foreign_entity_issues": rng.choice([None, "none"]),
            "government_entities": [{"id": rng.randint(1, 9), "name": "SENATE"}, None][: rng.randint(0, 2)],
            "lobbyists": [
                {"lobbyist": {"id": 100 * i + p, "first_name": "A", "last_name": f"L{p}"},
                 "covered_position": rng.choice([None, "Aide"]), "new": rng.choice([True, False, None])}
                for p in range(rng.randint(0, 4))
            ],
        }
        for a in range(rng.randint(0, 3))
    ]
    return {
        "filing_uuid": f"uuid-{i}", "filing_type": "Q1", "filing_year": 2000 + i % 25, "filing_period": "first_quarter",
        "dt_posted": f"20{i % 25 + 10:02d}-0{i % 9 + 1}-1{i % 9}T10:13:41.{i % 1000000:06d}-05:00",
        "income": f"{i}.50" if i % 3 else None, "expenses": None if i % 2 else f"{i}00.25",
        "filing_document_url": f"https://example.test/{i}", "registrant": {"id": i, "name": f"R{i}"},
        "client": {"id": i + 1, "name": f"C{i}"}, "lobbying_activities": activities,
    }


def _tables(count: int = 60) -> dict[str, list[dict]]:
    rng = random.Random(7)
    filings = [_filing(i, rng) for i in range(count)]
    return {
        "lobbying_filings": [_shape(f) for f in filings],
        "lobbying_activities": [row for f in filings for row in _activity_rows(f)],
        "lobbying_activity_lobbyists": [row for f in filings for row in _lobbyist_rows(f)],
    }


def _edges(dataset: str) -> list[dict]:
    base = {"filing_uuid": "e"}
    if dataset == "lobbying_filings":
        cases = (
            [{"filing_year": v} for v in ["2024", "0007", "-0", "9223372036854775807", "9223372036854775808", "12x", "", " 5", "+5"]]
            + [{"income": v} for v in ["30000.00", "1E+3", ".5", "5.", "-0.00", "1" * 33, "1" * 32, "1.1234567", "0.000001", "007.5",
                                       "1_000", "NaN", " 1", "-5.25", "1.0000000"]]
            + [{"dt_posted": v} for v in [
                "2024-01-02T10:13:41-05:00", "2024-01-02 10:13:41Z", "2024-01-02T10:13:41.5+00:00", "2024-01-02T10:13:41.123456Z",
                "2024-01-02T24:00:00-05:00", "2024-01-02T10:13:60Z", "2024-02-30T10:00:00Z", "0999-01-01T00:00:00Z",
                "1000-01-01T00:00:00+23:59", "9999-12-31T23:59:59Z", "9998-12-31T23:59:59-23:59", "2024-01-02T10:13:41",
                "20240102T101341Z", "2024-01-02", "", "1969-12-31T23:59:59.999999Z", "2024-01-02T10:13:41.1234567Z",
                "2024-01-02T10:13:41+24:00", "2024-13-02T10:13:41Z"]]
            + [{"lobbying_activities_json": v} for v in [
                None, "[]", "[null]", '[{"general_issue_code":"A"}]', "[1]", "not json", "{}", "null",
                '[{"description":"esc \\u001b done"}]', '[{"description":"tab\\tnew\\nline"}]', '[{"description":5}]']]
            + [{"government_entities_json": v} for v in ['[{"id":2,"name":"X"}]', '[{"id":"2"}]', '[{"id":2.5}]', "[null,{}]"]]
            + [{"registrant_name": v} for v in ["a\x0bb", "a\x1bb", "a\x7fb", "q\"b\\c", "\U0001f600", "", " "]]
            + [{"filing_uuid": v} for v in [None, "", "u\x1b"]]
        )
    elif dataset == "lobbying_activities":
        cases = ([{"activity_index": v} for v in ["0", "7", None, "x", "-1", "9223372036854775808"]]
                 + [{"description": "d\x1b"}, {"government_entities_json": '[{"id":1}]'}, {"government_entities_json": "garbage"}])
        base["activity_index"] = "0"
    else:
        cases = ([{"new": v} for v in ["true", "True", "1", "false", "False", "0", "yes", "", None]]
                 + [{"lobbyist_index": v} for v in ["0", None, "z"]] + [{"last_name": "O\x0e"}])
        base.update(activity_index="0", lobbyist_index="0")
    return [{**base, **case} for case in cases]


def _source(tmp_path: Path, dataset: str, rows: list[dict]) -> Path:
    path = tmp_path / f"{dataset}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=gr._legacy_schema(dataset)), path)
    return path


def _write_both(tmp_path: Path, dataset: str, source: Path, generation: str = GENERATION):
    context = {"generation_id": generation, "digest": gr._digest(source)}
    row = tmp_path / "row"
    bulk = tmp_path / "bulk"
    row.mkdir()
    bulk.mkdir()
    gr._write_by_row(source, row / "s.parquet", row / "r.parquet", dataset, **context)
    routed = government_bulk.write_dataset(
        source, bulk / "s.parquet", bulk / "r.parquet", dataset=dataset, policy=gr.POLICIES[dataset],
        generation_id=generation, digest=context["digest"],
        row_attempt=lambda raw, ordinal: gr.attempt(dataset, raw, ordinal, **context))
    return row, bulk, routed


@pytest.mark.parametrize("dataset", ["lobbying_filings", "lobbying_activities", "lobbying_activity_lobbyists"])
def test_the_bulk_writer_equals_the_row_writer_on_real_shaped_rows_and_every_edge(tmp_path, dataset):
    rows = _tables()[dataset] + _edges(dataset)
    source = _source(tmp_path, dataset, rows)
    row, bulk, routed = _write_both(tmp_path, dataset, source)
    row_subjects, bulk_subjects = pq.read_table(row / "s.parquet"), pq.read_table(bulk / "s.parquet")
    assert bulk_subjects.schema.equals(row_subjects.schema, check_metadata=True)
    assert bulk_subjects.to_pylist() == row_subjects.to_pylist()
    assert pq.read_table(bulk / "r.parquet").schema.equals(RECEIPT_SCHEMA)
    assert pq.read_table(bulk / "r.parquet").to_pylist() == pq.read_table(row / "r.parquet").to_pylist()
    receipts = pq.read_table(bulk / "r.parquet").to_pylist()
    refused = [r["outcome"] for r in receipts if r["outcome"] == "refused"]
    assert refused and receipts[-len(refused):] == [r for r in receipts if r["outcome"] == "refused"]  # refusals last
    assert 0 < routed < len(rows)  # the edges routed; the real-shaped rows did not


def test_a_bulk_bundle_is_admitted_without_any_row_route(tmp_path):
    dataset = "lobbying_filings"
    source = _source(tmp_path, dataset, _tables()[dataset])
    row, bulk, routed = _write_both(tmp_path, dataset, source)
    assert routed == 0
    messages = []
    sink = logger.add(messages.append, level="WARNING")
    try:
        etl_bulk.validate_bundle({dataset: [bulk / "s.parquet"]}, [bulk / "r.parquet"], [gr.POLICIES[dataset]],
                                 generation_id=GENERATION)
    finally:
        logger.remove(sink)
    assert not messages


def test_the_row_context_with_a_control_character_routes_every_row(tmp_path):
    dataset = "lobbying_activities"
    rows = _tables(10)[dataset]
    source = _source(tmp_path, dataset, rows)
    row, bulk, routed = _write_both(tmp_path, dataset, source, generation="odd\x1bgeneration")
    assert routed == len(rows)
    assert pq.read_table(bulk / "r.parquet").to_pylist() == pq.read_table(row / "r.parquet").to_pylist()


def test_a_dataset_with_its_own_mapper_rules_stays_on_the_row_path_loudly(tmp_path, monkeypatch):
    dataset = "gao_reports"
    source = _source(tmp_path, dataset, [{"report_id": "R"}])
    with pytest.raises(etl_bulk.NotBulkEligible):
        government_bulk.check_eligible(dataset, pq.read_schema(source))
    messages = []
    sink = logger.add(lambda m: messages.append(str(m)), level="WARNING")
    try:
        gr.migrate_outputs((source,), generation_id="loud")
        assert any("route=row dataset=gao_reports" in m for m in messages)
        _source(tmp_path, dataset, [{"report_id": "R"}])
        with etl_bulk.bulk_limits(threads=1, memory_limit="1GB", require_bulk=True):
            gr.migrate_outputs((source,), generation_id="strict")
        assert any("dataset=gao_reports rows=1" in m for m in messages)
    finally:
        logger.remove(sink)


def _generic_row(dataset: str, n: int) -> dict:
    row = {}
    for column in LEGACY_COLUMNS[dataset]:
        if column in DATE_FIELDS.get(dataset, ()):
            row[column] = "2025-01-0%d" % (n % 9 + 1)
        elif column in INTEGER_FIELDS.get(dataset, ()):
            row[column] = str(n)
        elif column in BOOLEAN_FIELDS.get(dataset, ()):
            row[column] = ["true", "0", None][n % 3]
        elif column in MONEY_FIELDS.get(dataset, ()):
            row[column] = f"{n}.25"
        elif column in TIMESTAMP_FIELDS.get(dataset, ()):
            row[column] = f"2025-01-0{n % 9 + 1}T01:02:03Z"
        elif column.endswith("_json"):
            row[column] = json.dumps([f"v{n}", None])
        else:
            row[column] = f"{column}-{n}"
    return row


@pytest.mark.parametrize("dataset", ["crs_reports", "sam_entities", "gao_recommendations", "fcc_proceedings"])
def test_the_other_declared_datasets_equal_the_row_writer(tmp_path, dataset):
    rows = [_generic_row(dataset, n) for n in range(30)]
    bad = dict(rows[0])
    bad[IDENTITY_FIELDS[dataset][0]] = ""
    days = ["0000-01-01", "0001-01-01", "9999-12-31", "2024-02-30", "2024-02-29", "20240102", "2024-1-2", "", " 2024-01-02"]
    column = next((c for c in DATE_FIELDS.get(dataset, ())), None)
    edges = [{**rows[0], column: day} for day in days] if column else []
    source = _source(tmp_path, dataset, rows + [bad] + edges)
    row, bulk, routed = _write_both(tmp_path, dataset, source)
    assert pq.read_table(bulk / "s.parquet").to_pylist() == pq.read_table(row / "s.parquet").to_pylist()
    assert pq.read_table(bulk / "r.parquet").to_pylist() == pq.read_table(row / "r.parquet").to_pylist()
    assert routed >= 1


@pytest.mark.parametrize("dataset", ["lobbying_filings", "lobbying_activities", "lobbying_activity_lobbyists"])
def test_the_bulk_restore_equals_the_source_and_the_row_restore(tmp_path, dataset):
    clean = [row for row in _tables()[dataset]]
    odd = [{**clean[0], "filing_uuid": "odd\x1b"}] if dataset == "lobbying_filings" else []
    source = _source(tmp_path, dataset, clean + odd)
    expected = pq.read_table(source)
    metadata = gr.migrate_outputs((source,), generation_id="first")
    restored = gr.internal_prior(dataset, source)
    assert pq.read_table(restored).equals(expected)
    # The row reader restores the same rows.
    policy = gr._prior_policy(dataset, source)
    selected = source.parent / "selected.parquet"
    from spicy_regs.etl_receipts import read_with_receipts, select_receipts

    select_receipts(Path(metadata["receipt_path"]), selected, dataset=dataset)
    by_row = [r["raw_record"] for r in read_with_receipts([source], [selected], policy, generation_id="first")]
    assert [dict(zip(expected.column_names, values)) for values in zip(*(expected[c].to_pylist() for c in expected.column_names))] == by_row


def test_the_restore_decodes_a_receipt_that_is_not_the_plain_shape_in_python(tmp_path):
    dataset = "lobbying_activities"
    rows = _tables(5)[dataset]
    source = _source(tmp_path, dataset, rows)
    gr.migrate_outputs((source,), generation_id="g")
    selected = tmp_path / "sel.parquet"
    from spicy_regs.etl_receipts import select_receipts

    select_receipts(tmp_path / "etl_receipts.parquet", selected, dataset=dataset)
    destination = tmp_path / "out.parquet"
    left = government_bulk.restore_legacy(source, selected, destination, dataset=dataset, policy=gr.POLICIES[dataset],
                                          generation_id="g", schema=gr._legacy_schema(dataset))
    assert left == 0 and pq.read_table(destination).num_rows == len(rows)


def test_mapped_dataset_refuses_changed_source_schema_when_bulk_is_required(tmp_path):
    source = tmp_path / "crs_reports.parquet"
    pq.write_table(pa.table({"report_id": [1]}), source)
    with etl_bulk.bulk_limits(threads=1, memory_limit="1GB", require_bulk=True):
        with pytest.raises(ValueError, match="reference fallback disabled"):
            gr.migrate_outputs((source,), generation_id="refuse-mapped-row-route")


def test_stripped_text_is_the_same_in_both_writers_and_the_receipt_keeps_it_as_written(tmp_path):
    """fcc_proceedings ``rule_section`` (owner, 2026-10-31): edges stripped, blank as NULL, the original in the receipt.

    Spellings from the retained 2026-10-08 walk (a single space, `` Part 76``) plus the edges the two writers could
    disagree on: every ASCII whitespace character DuckDB's ``trim`` is given, and non-ASCII spaces, which stay."""
    from spicy_regs.etl_receipts import _unpack

    spellings = [None, "", " ", "   ", " Part 76", "Part 76 ", "\t73.202(B)\r\n", "\v\f", "PART 80 AND 87",
                 "\xa0Part 1\xa0", " ", "a  b"]
    rows = [{**_generic_row("fcc_proceedings", n), "rule_section": value} for n, value in enumerate(spellings)]
    source = _source(tmp_path, "fcc_proceedings", rows)
    row, bulk, routed = _write_both(tmp_path, "fcc_proceedings", source)
    published = [r["rule_section"] for r in pq.read_table(bulk / "s.parquet").to_pylist()]
    assert published == [None, None, None, None, "Part 76", "Part 76", "73.202(B)", None, "PART 80 AND 87",
                         "\xa0Part 1\xa0", " ", "a  b"]
    assert pq.read_table(bulk / "s.parquet").to_pylist() == pq.read_table(row / "s.parquet").to_pylist()
    receipts = pq.read_table(bulk / "r.parquet").to_pylist()
    assert receipts == pq.read_table(row / "r.parquet").to_pylist()
    kept = [dict(_unpack(json.loads(r["processing_json"]))["raw_record"])["rule_section"] for r in receipts]
    assert kept == spellings
    # Plain spellings stay on the SQL path; only what DuckDB spells differently in JSON reaches the row code.
    assert routed <= 2
