"""The attribute tables' typed bulk writer and restore equal the row writer and row reader."""
import datetime as dt
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, etl_receipts, regulations_bulk
from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
from spicy_regs.transforms import regulations_receipts
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, policy, write_held_dataset
from spicy_regs.transforms.regulations_shape import TYPES

UTC = dt.timezone.utc
DISPLAY = json.dumps([{"name": "n", "label": "l", "tooltip": "t"}])


def _table(dataset, rows):
    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[dataset]])
    full = [{name: row.get(name) for name in schema.names} for row in rows]
    return pa.Table.from_pylist(full, schema=schema)


def _comment_rows():
    stamp = dt.datetime(2021, 5, 1, 12, 30, 5, tzinfo=UTC)
    return [
        {"comment_id": "plain", "page_count": 3, "withdrawn": False, "postmark_date": stamp,
         "display_properties_json": DISPLAY, "file_formats_json": '[{"fileUrl":"u","format":"pdf","size":3}]'},
        {"comment_id": "micro", "page_count": 0, "withdrawn": True, "postmark_date": stamp.replace(microsecond=5),
         "display_properties_json": DISPLAY},
        {"comment_id": "nulls"},
        {"comment_id": "escape", "address1": "x\x1b[0m", "city": "é😀", "display_properties_json": "null"},
        {"comment_id": "empty-list", "display_properties_json": "[]", "file_formats_json": "[null]"},
        {"comment_id": "bad-json", "display_properties_json": "[bad"},
        {"comment_id": "unknown-key", "display_properties_json": '[{"nope":1}]'},
        {"comment_id": "old", "postmark_date": dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)},
        {"comment_id": "year-1000", "postmark_date": dt.datetime(1000, 1, 1, tzinfo=UTC)},
        {"comment_id": "year-999", "postmark_date": dt.datetime(999, 1, 1, tzinfo=UTC)},
        {"comment_id": None, "page_count": 1},
        {"comment_id": "last", "page_count": 2**31 - 1, "display_properties_json": DISPLAY},
    ]


def _same(left, right):
    for a, b in zip(left, right):
        assert pq.read_table(a).equals(pq.read_table(b), check_metadata=True)


@pytest.mark.parametrize("dataset, rows", [
    ("comment_attributes", _comment_rows()),
    ("docket_attributes", [
        {"docket_id": "d1", "keywords": ["a", None, "a"], "effective_date": dt.datetime(2020, 2, 29, tzinfo=UTC),
         "display_properties_json": DISPLAY},
        {"docket_id": "d2", "keywords": [], "category": "x\x0b"}, {"docket_id": "d3"}]),
    ("document_attributes", [
        {"document_id": "x1", "allow_late_comments": True, "authors": ["z"], "author_date": dt.datetime(2020, 1, 1, tzinfo=UTC)},
        {"document_id": "x2"}]),
])
def test_typed_bulk_pair_and_restore_equal_the_row_writer_and_reader(tmp_path, monkeypatch, dataset, rows):
    source = tmp_path / "source.parquet"
    table = _table(dataset, rows)
    pq.write_table(table, source, row_group_size=3)
    reference = write_held_dataset(dataset, source, tmp_path / "row", bulk=False, generation_id="g")
    monkeypatch.setattr(regulations_bulk, "_BATCH", 4)
    routes = []
    original = regulations_bulk.write_held_dataset
    monkeypatch.setattr(regulations_bulk, "write_held_dataset", lambda *a, **k: routes.append("bulk") or original(*a, **k))
    # The bulk pair must not be redone by row, nor validated by row.
    monkeypatch.setattr(regulations_receipts, "write_records",
                        lambda *a, **k: pytest.fail("whole-pair row fallback"))
    actual = write_held_dataset(dataset, source, tmp_path / "bulk", bulk=True, generation_id="g")
    assert routes == ["bulk"]
    _same(reference, actual)
    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows",
                        lambda *a, **k: pytest.fail("row validation of a bundle the SQL validator proves"))
    etl_receipts.validate_receipt_bundle({dataset: [actual[0]]}, [actual[1]], [policy(dataset)], generation_id="g")

    selected = ReceiptInput(dataset, (actual[0],), actual[1], "g")
    monkeypatch.undo()
    row_restore = materialize_internal(selected, tmp_path / "row-restore.parquet", bulk=False, source_schema=table.schema)
    monkeypatch.setattr(regulations_bulk, "_BATCH", 4)
    restored = []
    restore = regulations_bulk.materialize_internal
    monkeypatch.setattr(regulations_bulk, "materialize_internal", lambda *a, **k: restored.append(1) or restore(*a, **k))
    bulk_restore = materialize_internal(selected, tmp_path / "bulk-restore.parquet", bulk=True, source_schema=table.schema)
    assert restored == [1]
    assert pq.read_table(row_restore).equals(pq.read_table(bulk_restore), check_metadata=True)


def test_a_restore_whose_receipt_names_another_subject_is_refused_not_misread(tmp_path):
    source = tmp_path / "source.parquet"
    table = _table("comment_attributes", [{"comment_id": f"c{i}", "page_count": i} for i in range(5)])
    pq.write_table(table, source)
    subject, receipts = write_held_dataset("comment_attributes", source, tmp_path / "native", bulk=True, generation_id="g")
    # Reorder the subjects: the receipts no longer follow them, so the positional pairing must be refused.
    shuffled = tmp_path / "shuffled.parquet"
    pq.write_table(pq.read_table(subject).take([1, 0, 2, 3, 4]), shuffled)
    with pytest.raises((etl_bulk.NotBulkEligible, ValueError)):
        regulations_bulk.materialize_internal(ReceiptInput("comment_attributes", (shuffled,), receipts, "g"),
                                              tmp_path / "out.parquet", source_schema=table.schema)


def test_a_forged_subject_is_refused_and_leaves_no_file(tmp_path):
    source = tmp_path / "source.parquet"
    table = _table("comment_attributes", [{"comment_id": f"c{i}", "page_count": i, "city": f"town{i}"} for i in range(4)])
    pq.write_table(table, source)
    subject, receipts = write_held_dataset("comment_attributes", source, tmp_path / "native", bulk=True, generation_id="g")
    changed = pq.read_table(subject).to_pylist()
    changed[2]["city"] = "elsewhere"
    forged = tmp_path / "forged.parquet"
    pq.write_table(pa.Table.from_pylist(changed, schema=pq.read_schema(subject)), forged)
    with pytest.raises((etl_bulk.NotBulkEligible, ValueError)):
        regulations_bulk.materialize_internal(ReceiptInput("comment_attributes", (forged,), receipts, "g"),
                                              tmp_path / "out.parquet", source_schema=table.schema)
    assert not (tmp_path / "out.parquet").exists()


def test_the_bulk_validator_proves_datetime_evidence_and_leaves_the_rest_to_the_row_decoder():
    import duckdb
    con = duckdb.connect()

    def proven(value):
        text = json.dumps(["datetime", value]).replace(" ", "")
        result = con.execute("SELECT " + etl_bulk.decodes_sql("'" + text.replace("'", "''") + "'")).fetchone()
        assert result is not None
        return result[0]
    assert proven("2021-05-01T12:30:05+00:00") and proven("1000-01-01T00:00:00.000005+00:00")
    for unproven in ("2021-02-30T00:00:00+00:00", "2021-05-01T24:00:00+00:00", "0999-01-01T00:00:00+00:00",
                     "2021-05-01T12:30:05+05:00", "2021-05-01 12:30:05+00:00"):
        assert not proven(unproven)


def _comment(comment_id, **attributes):
    return {"data": {"id": comment_id, "type": "comments", "attributes": attributes}}


def _fresh(tmp_path, name, records):
    from tests.test_regulations_attributes import _parts
    from spicy_regs.transforms.regulations_attributes import merge_attribute_parts
    _parts(tmp_path / name, "comment_attributes", records)
    out = tmp_path / f"{name}.parquet"
    merge_attribute_parts("comment_attributes", tmp_path / name / "comment_attributes", None, out)
    return out


def _receipts_by_id(subject_path, receipts_path):
    receipts = pq.read_table(receipts_path).to_pylist()
    ids = {r["record_id"]: r for r in receipts if r["outcome"] == "accepted"}
    order = [json.loads(r["identity_json"])[1][0][1][1][1] for r in receipts if r["outcome"] == "accepted"]
    return ids, order, receipts


def test_the_nights_attribute_rows_upsert_over_the_selected_native_table_without_restoring_it(tmp_path, monkeypatch):
    """The receipt route's upsert, still the docket and document attributes' route, proven on comment-shaped rows.

    comment_attributes itself publishes without receipts (ss-vse3; tests/test_comment_attributes_receipt_switch.py),
    so its marker is set aside here for the whole test, as it was before the switch.
    """
    from spicy_regs import subject_catalog
    from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset, finish_dataset, restore_dataset
    from spicy_regs.selected_generations import SelectedInputs

    retired = subject_catalog.retired_receipt_datasets() - {"comment_attributes"}
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired)

    root = tmp_path / "root"
    seed = _fresh(tmp_path, "seed", [
        _comment(f"C-{i}", pageCount=i, city=f"town{i}", withdrawn=False,
                 displayProperties=[{"name": "n", "label": "l", "tooltip": "t"}]) for i in range(6)])
    # The table was converted from a file that carried metadata: the night's rows state the same, or no restore agrees.
    pq.write_table(pq.read_table(seed).replace_schema_metadata({"placement": "kept"}), seed)
    finish_dataset(root, "comment_attributes", seed, publish=False)
    inputs = SelectedInputs(root, root / "selected", public_url="")
    prior = inputs.select("comment_attributes")
    before, _, _ = _receipts_by_id(prior.subjects[0], prior.receipts)

    fresh = _fresh(tmp_path, "night", [
        _comment("C-2", pageCount=2, city="town2", withdrawn=False,
                 displayProperties=[{"name": "n", "label": "l", "tooltip": "t"}]),   # refetched, unchanged
        _comment("C-4", pageCount=40, city="moved", withdrawn=False),              # refetched, changed
        _comment("C-9", pageCount=9, postmarkDate="2021-01-01T05:00:00Z"),          # new
        _comment("A-0", pageCount=1),                                               # new, sorts first
    ])
    # Nothing is restored to rows, nor re-receipted beyond the night's four rows.
    monkeypatch.setattr(regulations_receipts, "_qualified_rows", lambda *a, **k: pytest.fail("restored by row"))
    monkeypatch.setattr(regulations_bulk, "materialize_internal", lambda *a, **k: pytest.fail("restored"))
    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows",
                        lambda *a, **k: pytest.fail("row validation of a bundle the SQL validator proves"))
    finish_attribute_dataset(root, "comment_attributes", fresh, publish=False, inputs=inputs)
    monkeypatch.undo()
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired)

    inputs = SelectedInputs(root, root / "selected-after", public_url="")
    after = inputs.select("comment_attributes")
    assert after.generation_id != prior.generation_id
    table = pq.read_table(after.subjects[0])
    assert table.schema.equals(policy("comment_attributes").subject_schema)
    assert table["comment_id"].to_pylist() == ["A-0", "C-0", "C-1", "C-2", "C-3", "C-4", "C-5", "C-9"]
    assert {r["comment_id"]: r["city"] for r in table.to_pylist()}["C-4"] == "moved"
    now, order, receipts = _receipts_by_id(after.subjects[0], after.receipts)
    assert order == table["comment_id"].to_pylist(), "accepted receipts follow their subjects"
    by_id = {json.loads(r["identity_json"])[1][0][1][1][1]: r for r in receipts if r["outcome"] == "accepted"}
    prior_by_id = {json.loads(r["identity_json"])[1][0][1][1][1]: r
                   for r in pq.read_table(prior.receipts).to_pylist() if r["outcome"] == "accepted"}
    for kept in ("C-0", "C-1", "C-3", "C-5", "C-2"):   # kept rows and the unchanged refetch: the prior receipt, verbatim
        assert by_id[kept] == prior_by_id[kept]
    assert by_id["C-4"]["receipt_id"] != prior_by_id["C-4"]["receipt_id"]
    assert prior_by_id["C-4"]["receipt_id"] in by_id["C-4"]["diagnostic_json"], "a change names its predecessor"
    assert {by_id[i]["generation_id"] for i in ("C-4", "C-9", "A-0")} == {by_id["C-4"]["generation_id"]}
    assert by_id["C-4"]["generation_id"] != prior_by_id["C-0"]["generation_id"]
    assert len(before) == 6 and len(now) == 8

    # The next night's restore reproduces the table from the merged receipts.
    restored = tmp_path / "restored.parquet"
    assert restore_dataset(root, "comment_attributes", restored, inputs=inputs)
    assert pq.read_table(restored)["comment_id"].to_pylist() == table["comment_id"].to_pylist()
    assert pq.read_table(restored).to_pylist()[5]["page_count"] == 40
    assert pq.read_schema(restored).metadata == {b"placement": b"kept"}
