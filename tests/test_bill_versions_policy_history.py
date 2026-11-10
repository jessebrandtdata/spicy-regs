"""A bill_versions /1 generation stays a readable prior after /2 appended publication evidence (ss-m4r1)."""

from copy import deepcopy
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS

from spicy_regs import legislative_documents as ld, legislative_receipts as lr
from spicy_regs.etl_receipts import decode_exact_json, receipt_policies, selected_subject_policy

DATASET = "bill_versions"
APPENDED = ("publication_dates", "publication_evidence_status")
APPENDED_SOURCE = ("publication_dates_json", "publication_evidence_status", "publication_evidence_json")


def test_history_differs_only_by_the_two_appended_nullable_columns():
    current, previous = receipt_policies(lr.policy(DATASET))
    assert (current.policy_version, previous.policy_version) == ("legislative-documents/2", "legislative-documents/1")
    assert previous.subject_schema.names == [n for n in current.subject_schema.names if n not in APPENDED]
    assert all(current.subject_schema.field(name).nullable for name in APPENDED)
    # Only the exact recorded descriptor reads its predecessor.
    changed = replace(current, identity_fields=current.identity_fields[:-1])
    assert receipt_policies(changed) == (changed,)


def source_rows():
    columns = [c for c in TABLE_CONTRACTS[DATASET].columns if c not in APPENDED_SOURCE]
    base = dict.fromkeys(columns) | {
        "bill_id": "119-hjres-25", "source": "govinfo", "package_id": "BILLS-119hjres25enr",
        "sha256": "sha256:" + "a" * 64, "observed_at": "2026-09-12T12:29:18+00:00", "section_count": "4",
    }
    return [base | {"version_code": "enrolled-bill", "version_date": ""},
            base | {"version_code": "introduced", "version_date": "2025-03-01", "package_id": "BILLS-119hjres25ih"}]


@pytest.fixture
def earlier_bundle(tmp_path, monkeypatch):
    """A /1 bundle written by the maintained writer under the exact registry /1 shipped with."""
    raw = source_rows()
    schema = pa.schema([(name, pa.string()) for name in raw[0]])
    source = tmp_path / f"{DATASET}.parquet"
    pq.write_table(pa.Table.from_pylist(raw, schema=schema), source)
    previous = receipt_policies(lr.policy(DATASET))[1]
    old_registry = deepcopy(ld.field_registry())
    old_registry[DATASET]["fields"] = [f for f in old_registry[DATASET]["fields"] if f["name"] not in APPENDED_SOURCE]
    current_policy, current_processor = lr.policy, lr._processor
    uncached_schema = ld.subject_schema.__wrapped__
    with monkeypatch.context() as patch:
        for module in (ld, lr):
            patch.setattr(module, "field_registry", lambda: old_registry)
            patch.setattr(module, "subject_schema", uncached_schema)
        patch.setattr(lr, "policy", lambda name: previous if name == DATASET else current_policy(name))
        patch.setattr(lr, "_processor", lambda name: previous.policy_version if name == DATASET else current_processor(name))
        lr.write_legislative_outputs([source], tmp_path / "old", generation_id="old", bulk=False)
    bundle = tmp_path / "old"
    assert pq.read_schema(bundle / f"{DATASET}.parquet").equals(previous.subject_schema)
    return bundle, raw, schema


@pytest.mark.parametrize("bulk", [False, True])
def test_earlier_generation_restores_then_carries_as_unrefreshed_rows(tmp_path, earlier_bundle, bulk):
    bundle, raw, schema = earlier_bundle
    assert selected_subject_policy(lr.policy(DATASET), [bundle / f"{DATASET}.parquet"]).policy_version == \
        "legislative-documents/1"
    restored = lr.restore_prior(bundle, tmp_path / "restored", bulk=bulk)[DATASET]
    original = pq.read_table(restored)
    assert original.schema.equals(schema) and original.to_pylist() == raw

    # The builder's shared merge NULL-fills columns a prior predates; a /2 row no refresh selected holds the same NULLs.
    carried = tmp_path / "carried" / f"{DATASET}.parquet"
    carried.parent.mkdir()
    rows = [row | dict.fromkeys(APPENDED_SOURCE) for row in raw]
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(n, pa.string()) for n in rows[0]])), carried)
    manifest = lr.write_legislative_outputs([carried], tmp_path / "new", generation_id="new", bulk=bulk)
    assert not any(manifest["refused_rows"].values())
    published = pq.read_table(tmp_path / "new" / f"{DATASET}.parquet")
    assert published.schema.equals(lr.policy(DATASET).subject_schema)
    assert all(row[name] is None for row in published.to_pylist() for name in APPENDED)
    receipts = [r for r in pq.read_table(tmp_path / "new/etl_receipts.parquet").to_pylist()
                if r["dataset"] == DATASET and r["outcome"] == "accepted"]
    assert {r["policy_version"] for r in receipts} == {"legislative-documents/2"}
    assert sorted((decode_exact_json(r["processing_json"])[lr.RAW] for r in receipts), key=str) == sorted(rows, key=str)
