"""Literal funding labels and exact reads of the earlier integer publication."""

from copy import deepcopy
from dataclasses import replace
import hashlib

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.senate_expenditure_tables import page_context
from spicy_regs import legislative_documents as ld, legislative_receipts as lr
from spicy_regs.contract_types import arrow_schema
from spicy_regs.etl_receipts import decode_exact_json, receipt_policies


DATASET = "senate_expenditures"
FUNDING = ("funding_year", "funding_year_end")


def source_row(year, end=None, ordinal=0):
    return dict.fromkeys(f["name"] for f in ld.field_registry()[DATASET]["fields"]) | {
        "package_id": "GPO-CDOC-110sdoc19", "file_name": "part1.pdf", "page": "1",
        "table_ordinal": "0", "row_ordinal": str(ordinal), "text_sha256": "a" * 64,
        "funding_year": year, "funding_year_end": end,
        "cells_json": '["source cell"]', "amounts_json": '[[{"text":"-1.20","amount":"-1.20"}]]',
    }


@pytest.mark.parametrize("label,expected", [
    ("X", ("X", None)), ("X (NO-YEAR)", ("X", None)), ("X (REVOLVING)", ("X", None)),
    ("0509", ("0509", None)), ("2021", ("2021", None)), ("2021-2023", ("2021", "2023")),
    ("0509-0511", ("0509", "0511")),
])
def test_source_funding_labels_survive_actual_subject_mapping(label, expected):
    context = page_context(f"OFFICE\nFunding Year {label}\nB-1")
    raw = source_row(context.funding_year, context.funding_year_end)
    mapped = lr.mapped_record(DATASET, raw)
    assert tuple(mapped[name] for name in FUNDING) == expected
    assert mapped[lr.RAW] == raw
    policy = lr.policy(DATASET)
    assert policy.policy_version == "legislative-documents/2"
    for name in FUNDING:
        assert policy.subject_schema.field(name).type == arrow_schema(TABLE_CONTRACTS[DATASET]).field(name).type == pa.string()
    assert pa.Table.from_pylist([mapped], schema=policy.subject_schema).to_pylist()[0] == {
        name: mapped[name] for name in policy.subject_schema.names
    }


def test_senate_policy_history_changes_only_the_two_funding_types_and_version():
    current, previous = receipt_policies(lr.policy(DATASET))
    assert previous.policy_version == "legislative-documents/1"
    schema = previous.subject_schema
    for name in FUNDING:
        at = schema.get_field_index(name)
        assert schema.field(at).type == pa.int64()
        schema = schema.set(at, schema.field(at).with_type(pa.string()))
    assert replace(previous, subject_schema=schema, policy_version=current.policy_version).descriptor() == current.descriptor()
    with pytest.raises(ValueError, match="exact declaration"):
        receipt_policies(replace(current, identity_fields=current.identity_fields[:-1]))


def make_earlier_bundle(tmp_path, monkeypatch, raw, schema):
    """Produce a /1 bundle through the maintained original integer mapping."""
    previous = receipt_policies(lr.policy(DATASET))[1]
    source = tmp_path / f"{DATASET}.parquet"
    pq.write_table(pa.Table.from_pylist(raw, schema=schema), source)
    old_registry = deepcopy(ld.field_registry())
    for field in old_registry[DATASET]["fields"]:
        if field["name"] in FUNDING:
            field.update(target_type="int64", decision="flatten")
    current_policy, current_processor = lr.policy, lr._processor
    uncached_schema = ld.subject_schema.__wrapped__
    with monkeypatch.context() as patch:
        for module in (ld, lr):
            patch.setattr(module, "field_registry", lambda: old_registry)
            patch.setattr(module, "subject_schema", uncached_schema)
        patch.setattr(lr, "policy", lambda name: previous if name == DATASET else current_policy(name))
        patch.setattr(lr, "_processor", lambda name: previous.policy_version if name == DATASET else current_processor(name))
        lr.write_legislative_outputs([source], tmp_path / "old", generation_id="old", bulk=False)
    return tmp_path / "old"


@pytest.fixture
def earlier_bundle(tmp_path, monkeypatch):
    raw = [source_row("2021", "2023"), source_row("0509", ordinal=1), source_row("509", ordinal=2),
           source_row(None, ordinal=3)]
    schema = pa.schema([(name, pa.string()) for name in raw[0]], metadata={b"scope": b"earlier-fixture"})
    bundle = make_earlier_bundle(tmp_path, monkeypatch, raw, schema)
    subject = bundle / f"{DATASET}.parquet"
    values = pq.read_table(subject).to_pylist()
    assert [(r["funding_year"], r["funding_year_end"]) for r in values] == [(2021, 2023), (509, None), (509, None), (None, None)]
    return bundle, raw, schema


@pytest.mark.parametrize("bulk", [False, True])
def test_historical_numeric_raw_restores_without_becoming_new_literal_text(tmp_path, monkeypatch, bulk):
    raw = [source_row(509, 511), source_row(2021, 2023, ordinal=1), source_row(None, ordinal=2)]
    schema = pa.schema([(name, pa.int64() if name in FUNDING else pa.string()) for name in raw[0]],
                       metadata={b"scope": b"earlier-numeric-fixture"})
    bundle = make_earlier_bundle(tmp_path, monkeypatch, raw, schema)
    held = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in bundle.iterdir()}
    subjects = pq.read_table(bundle / f"{DATASET}.parquet").to_pylist()
    assert [(r["funding_year"], r["funding_year_end"]) for r in subjects] == [(509, 511), (2021, 2023), (None, None)]
    current, previous = receipt_policies(lr.policy(DATASET))
    records = [s | {lr.RAW: r} for s, r in zip(subjects, raw, strict=True)]
    assert list(lr.retained_source_rows(DATASET, previous, records)) == raw
    restored = lr.restore_prior(bundle, tmp_path / "restored", bulk=bulk)
    original = pq.read_table(restored[DATASET])
    assert original.schema.equals(schema, check_metadata=True)
    assert original.to_pylist() == raw
    with pytest.raises(ld.LegislativeShapeError, match="expected string"):
        list(lr.retained_source_rows(DATASET, current, records))
    # The writer retains rejected source rows as refusal receipts rather than
    # aborting the bundle or inventing text from the old integer.
    manifest = lr.write_legislative_outputs([restored[DATASET]], tmp_path / "new", generation_id="new", bulk=bulk)
    assert manifest["refused_rows"] == {DATASET: 2}
    published = pq.read_table(tmp_path / "new" / f"{DATASET}.parquet").to_pylist()
    assert len(published) == 1 and all(published[0][name] is None for name in FUNDING)
    refused = [r for r in pq.read_table(tmp_path / "new/etl_receipts.parquet").to_pylist()
               if r["dataset"] == DATASET and r["outcome"] == "refused"]
    assert [decode_exact_json(r["processing_json"])[lr.RAW] for r in refused] == raw[:2]
    with pytest.raises(ValueError, match="conversion refusals cannot qualify"):
        lr.restore_prior(tmp_path / "new", tmp_path / "new-restored", bulk=bulk)
    assert held == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in bundle.iterdir()}


@pytest.mark.parametrize("value", [True, 509.0, "X", 2**63])
def test_selected_historical_policy_preserves_its_integer_refusals(value):
    with pytest.raises(ld.LegislativeShapeError, match="expected integer|cannot be represented exactly"):
        ld.recorded_subject(DATASET, source_row(value), {}, policy_version="legislative-documents/1")


@pytest.mark.parametrize("bulk", [False, True])
def test_retained_integer_bundle_restores_exact_raw_then_builds_new_strings(tmp_path, earlier_bundle, bulk):
    bundle, raw, schema = earlier_bundle
    held = {str(p.relative_to(bundle)): hashlib.sha256(p.read_bytes()).hexdigest() for p in bundle.iterdir()}
    restored = lr.restore_prior(bundle, tmp_path / "restored", bulk=bulk)
    original = pq.read_table(restored[DATASET])
    assert original.schema.equals(schema, check_metadata=True)
    assert original.to_pylist() == raw
    # The two old integer 509 values have different, separately retained source spellings.
    # Restoration never invents a leading zero from the published integer.
    assert [r["funding_year"] for r in original.to_pylist()] == ["2021", "0509", "509", None]
    lr.write_legislative_outputs([restored[DATASET]], tmp_path / "new", generation_id="new",
                                prior_receipts=[bundle / "etl_receipts.parquet"], bulk=bulk)
    published = pq.read_table(tmp_path / "new" / f"{DATASET}.parquet")
    assert published.schema.equals(lr.policy(DATASET).subject_schema)
    assert [r["funding_year"] for r in published.to_pylist()] == ["2021", "0509", "509", None]
    assert held == {str(p.relative_to(bundle)): hashlib.sha256(p.read_bytes()).hexdigest() for p in bundle.iterdir()}
    lr.restore_prior(tmp_path / "new", tmp_path / "new-restored", bulk=bulk)


def test_selected_old_publication_stays_integer_and_cannot_claim_current_schema(tmp_path, monkeypatch, earlier_bundle):
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.generations import build_generation
    from spicy_regs.local_data import local_selection
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    bundle, raw, _ = earlier_bundle
    subject, receipts = bundle / f"{DATASET}.parquet", bundle / "etl_receipts.parquet"
    current = installed_policies()[DATASET]
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    with pytest.raises(ValueError, match="Subject schema differs"):
        build_generation(tmp_path / "false-current", family="senate-expenditures", files=[subject],
                         expected_keys=[subject.name], receipt_path=receipts,
                         receipt_policies=[current, lr.FILE_POLICY], receipt_generation_id="old")
    remember_selection(tmp_path, [SelectedDataset(DATASET, (subject,), receipts, "old")])
    selection = local_selection(tmp_path)
    selected = selection.native[DATASET]
    assert pq.read_schema(selection.paths(DATASET)[0]).field("funding_year").type == pa.int64()
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {}, "", local_native={DATASET: {
            "subjects": selected.subjects, "receipts": selected.receipts, "generation_id": selected.generation_id}})
        rows = list(adapter.selected_rows(DATASET))
    assert [row["funding_year"] for row in rows] == [2021, 509, 509, None]
    assert [row[lr.RAW] for row in rows] == raw


def test_historical_reconstruction_requires_exact_selected_policy_and_raw_values(earlier_bundle):
    bundle, raws, _ = earlier_bundle
    current, previous = receipt_policies(lr.policy(DATASET))
    subjects = pq.read_table(bundle / f"{DATASET}.parquet").to_pylist()
    records = [s | {lr.RAW: raw} for s, raw in zip(subjects, raws, strict=True)]
    assert list(lr.retained_source_rows(DATASET, previous, records)) == raws
    with pytest.raises(ValueError, match="exact declaration"):
        list(lr.retained_source_rows(DATASET, replace(current, policy_version=previous.policy_version), records))
    with pytest.raises(ValueError, match="disagrees"):
        list(lr.retained_source_rows(DATASET, current, records))
    wrong = records[1] | {lr.RAW: raws[1] | {"funding_year": "0609"}}
    with pytest.raises(ValueError, match="disagrees"):
        list(lr.retained_source_rows(DATASET, previous, [wrong]))
    with pytest.raises(ld.LegislativeShapeError, match="expected integer"):
        list(lr.retained_source_rows(DATASET, previous, [records[1] | {lr.RAW: raws[1] | {"funding_year": "X"}}]))
    with pytest.raises(ld.LegislativeShapeError, match="Unknown Senate"):
        ld.recorded_subject(DATASET, raws[0], subjects[0], policy_version="legislative-documents/unknown")


def test_current_mapping_refuses_numbers_without_recoverable_literal_source():
    with pytest.raises(ld.LegislativeShapeError, match="expected string"):
        ld.map_subject(DATASET, source_row(509))
