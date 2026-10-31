"""Receipt context builds no table of its own (ss-j4vz B11): a context input only carries its collections."""

import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_fec_identity_context import _emit_mapping, build_fec_identity_context
from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA
from spicy_regs.transforms.fec_identity_context_fields import REGISTRY


class RecordingWriter:
    """Fixture: records emits instead of writing receipts."""

    policies = {"fec_record_evidence": object()}

    def __init__(self):
        self.emitted = []

    def emit(self, table, row, *, input_witness, evidence=None, source_input=None):
        self.emitted.append((table, row.get("record_id"), [e["role"] for e in evidence or []]))


def evidence(table, record_id, role):
    return dict(target_table=table, target_record_id=record_id, role=role)


def test_no_registry_table_is_admitted_from_receipts():
    assert not any("admitted_from_receipts" in rules for rules in REGISTRY.values())
    assert not {"fec_research_response_outcomes", "fec_research_context_dispositions"} & REGISTRY.keys()


def test_context_input_carries_its_collections_and_builds_no_other_table(tmp_path):
    collection = dict.fromkeys(COLLECTION_SCHEMA.names)
    collection.update(collection_id="research-capture-0000", source_family="fec-query", record_count=1,
                      relationship_count=0, collection_outcome_json=json.dumps({"receiverDisposition": {}}))
    source = tmp_path / "collections.parquet"
    pq.write_table(pa.Table.from_pylist([collection], schema=COLLECTION_SCHEMA), source)
    manifest = tmp_path / "selection.json"
    manifest.write_text(json.dumps({
        "version": 1, "generation_id": "context-g1", "tables": ["fec_collections"],
        "inputs": [{"mode": "context", "table": "fec_collections", "path": str(source), "rows": 1,
                    "sha256": "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest(),
                    "source_generation_pin": "sha256:" + "a" * 64, "collection_ids": ["research-capture-0000"]}],
    }))
    outputs = build_fec_identity_context(manifest, tmp_path / "out")
    assert [path.name for path in outputs] == ["fec_collections.parquet", "etl_receipts.parquet"]
    assert sorted(p.name for p in (tmp_path / "out").glob("*.parquet")) == ["etl_receipts.parquet", "fec_collections.parquet"]
    receipts = pq.read_table(tmp_path / "out" / "etl_receipts.parquet").to_pylist()
    assert [r["dataset"] for r in receipts] == ["fec_collections"]


def test_a_mapping_may_emit_only_registered_tables():
    writer = RecordingWriter()
    with pytest.raises(ValueError, match="another owned family: fec_research_response_outcomes"):
        _emit_mapping(writer, {"fec_research_response_outcomes": [dict(record_id="r1")]}, [], {}, None)
    assert writer.emitted == []


def test_evidence_is_attached_per_row_by_table_and_record_identity():
    writer = RecordingWriter()
    tables = {"fec_filings": [dict(record_id="a"), dict(record_id="b")]}
    links = [
        evidence("fec_filings", "a", "primary"),
        evidence("fec_filings", "b", "primary"),
        evidence("fec_filings", "a", "field_definition"),
        evidence("fec_receipts", "a", "other_table"),
    ]
    _emit_mapping(writer, tables, links, {}, None)
    assert writer.emitted[:2] == [
        ("fec_filings", "a", ["primary", "field_definition"]),
        ("fec_filings", "b", ["primary"]),
    ]
    assert [e[0] for e in writer.emitted[2:]] == ["fec_record_evidence"] * 4


def test_selected_collections_are_filtered_before_python_in_input_order_and_hashed_once(tmp_path, monkeypatch):
    """ss-3bkk: one hash per input, Arrow filtering across batches, and a change after that hash is refused."""
    import os
    from itertools import islice

    from spicy_regs.transforms import assemble_fec_query
    from spicy_regs.transforms.build_fec_identity_context import _input_rows

    # Contiguous collections over misaligned row groups: statistics skip most groups, never a selected row.
    rows = [dict(collection_id=None if i % 97 == 0 else f"c{i // 500}", ordinal=i) for i in range(5000)]
    source = tmp_path / "records.parquet"
    pq.write_table(pa.Table.from_pylist(rows), source, row_group_size=700)
    item = dict(path=str(source), rows=len(rows), sha256="sha256:" + hashlib.sha256(source.read_bytes()).hexdigest())
    hashed = []
    original = assemble_fec_query._sha
    monkeypatch.setattr(assemble_fec_query, "_sha", lambda path: hashed.append(path) or original(path))
    selected = {"c3", "c7", "absent"}
    assert list(_input_rows(item, tmp_path, collections=selected)) == [r for r in rows if r["collection_id"] in selected]
    assert hashed == [source]
    assert list(_input_rows(item, tmp_path)) == rows
    stream = _input_rows(item, tmp_path)
    assert len(list(islice(stream, len(rows)))) == len(rows)
    os.utime(source, ns=(1, 1))
    with pytest.raises(ValueError, match="changed during mapping"):
        next(stream)
