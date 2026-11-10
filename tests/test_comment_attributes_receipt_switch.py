"""comment_attributes publishes without per-row receipts, and each sweep batch's upsert is the one it was (ss-vse3).

The fixture's rows are real: each is a published row's contract-shape copy as its 2026-11-10 receipt held it
(``raw_conversion_inputs``), taken by range from receipt row groups across the file (2,506 prior rows, six of them
with file formats; 600 fresh: 300 changed, 100 refetched unchanged, 200 new). ``expected/`` is what main 45e278f0's
receipt route made of them, as ``finish_attribute_dataset`` does: each file through ``write_held_dataset``, the prior
published cold, then the fresh pair upserted over it with ``native_merge``.

What these cannot see: the whole table (the full-size check is in docs/comment-attributes-receipt-archive.md), and
Parquet layout beyond rows and their order.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.receipt_switch import receipt_files, table_differences

FIXTURE = Path(__file__).parent / "fixtures/comment_attributes_switch"
DATASET = "comment_attributes"
KEY = ["comment_id"]


def _fresh_subjects(tmp_path: Path) -> Path:
    from spicy_regs.transforms.regulations_receipts import write_held_dataset

    subjects, _ = write_held_dataset(DATASET, FIXTURE / "fresh.parquet", tmp_path / "fresh", generation_id="fresh",
                                     bulk=True, validate=False)
    return subjects


def test_the_plain_upsert_is_the_receipt_routes_upsert(tmp_path):
    from spicy_regs.native_merge import upsert_subjects
    from spicy_regs.transforms.regulations_receipts import policy

    declared = policy(DATASET)
    merged = upsert_subjects(declared.subject_schema, identity=declared.identity_fields,
                             fresh=_fresh_subjects(tmp_path), prior=FIXTURE / "expected/prior.parquet",
                             order_by=declared.identity_fields, subjects=tmp_path / "merged.parquet")
    assert table_differences(merged.subjects, FIXTURE / "expected/merged.parquet", key=KEY) == []
    assert merged._asdict() | {"subjects": None} == {"subjects": None, **json.loads(
        (FIXTURE / "expected/counts.json").read_text())}
    assert receipt_files(tmp_path / "merged.parquet") == []


def _local_run(tmp_path: Path):
    """A local run beside its published table: the selection a retired table reads (``SelectedInputs.select``)."""
    from spicy_regs.selected_generations import SelectedInputs

    root = tmp_path / "run"
    root.mkdir()
    shutil.copyfile(FIXTURE / "expected/prior.parquet", root / f"{DATASET}.parquet")
    return root, SelectedInputs(root, tmp_path / "selected", public_url="")


def test_a_batch_upserts_the_table_and_writes_no_receipt_anywhere(tmp_path):
    from spicy_regs.generations import verify_generation
    from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset

    root, inputs = _local_run(tmp_path)
    built = finish_attribute_dataset(root, DATASET, FIXTURE / "fresh.parquet", publish=False, inputs=inputs)
    assert table_differences(built, FIXTURE / "expected/merged.parquet", key=KEY) == []
    [generation] = (root / "generations").iterdir()
    artifact = verify_generation(generation)
    assert "etlReceipts" not in artifact.root["spec"]
    assert set(artifact.root["spec"]["tables"]) == {f"{DATASET}.parquet"}
    assert not list(tmp_path.rglob("etl_receipts.parquet")), "the fresh rows' check receipts are never kept"
    # The next batch of the same run reads this batch's table, not the first prior.
    selected = inputs.select(DATASET)
    assert selected.receipts is None and selected.subjects == (generation / f"{DATASET}.parquet",)
    again = finish_attribute_dataset(root, DATASET, FIXTURE / "fresh.parquet", publish=False, inputs=inputs)
    assert table_differences(again, FIXTURE / "expected/merged.parquet", key=KEY) == []


def test_a_refused_row_refuses_the_batch_and_leaves_the_table(tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_attribute_dataset

    root, inputs = _local_run(tmp_path)
    table = pq.read_table(FIXTURE / "fresh.parquet")
    texts = table["display_properties_json"].to_pylist()
    texts[0] = '[{"name": "x", "unrepresented": 1}]'
    bad = tmp_path / "bad.parquet"
    pq.write_table(table.set_column(table.schema.get_field_index("display_properties_json"),
                                    "display_properties_json", pa.array(texts, pa.string())), bad)
    before = (root / f"{DATASET}.parquet").read_bytes()
    with pytest.raises(ValueError, match="native conversion refused"):
        finish_attribute_dataset(root, DATASET, bad, publish=False, inputs=inputs)
    assert (root / f"{DATASET}.parquet").read_bytes() == before
    assert not (root / "generations").exists() or not any((root / "generations").iterdir())


def test_an_upsert_refuses_a_repeated_identity_or_another_schema(tmp_path):
    from spicy_regs.native_merge import upsert_subjects
    from spicy_regs.transforms.regulations_receipts import policy

    declared = policy(DATASET)
    fresh = pq.read_table(_fresh_subjects(tmp_path))
    repeated = tmp_path / "repeated.parquet"
    pq.write_table(pa.concat_tables([fresh, fresh.slice(0, 1)]), repeated)
    def upsert(path):
        return upsert_subjects(declared.subject_schema, identity=declared.identity_fields, fresh=path,
                               prior=FIXTURE / "expected/prior.parquet", order_by=declared.identity_fields,
                               subjects=tmp_path / "out.parquet")

    with pytest.raises(ValueError, match="repeat an identity"):
        upsert(repeated)
    narrowed = tmp_path / "narrowed.parquet"
    pq.write_table(fresh.drop(["zip"]), narrowed)
    with pytest.raises(ValueError, match="differ from the table's schema"):
        upsert(narrowed)
    assert not (tmp_path / "out.parquet").exists()


def test_the_refresh_republishes_nothing_for_a_retired_attribute_table(tmp_path, monkeypatch):
    """The ETL's generation is the family's own; republishing would copy it, or state receipts admission refuses."""
    from spicy_regs.pipelines.rollups import regulatory_base
    from spicy_regs.pipelines.rollups.base import Unchanged
    from spicy_regs.subject_catalog import retired_receipt_datasets

    assert DATASET in retired_receipt_datasets()
    monkeypatch.setattr(regulatory_base, "SelectedPriors", lambda *a, **k: pytest.fail("nothing is selected"))
    with pytest.raises(Unchanged, match="publishes without receipts"):
        regulatory_base.CommentAttributesFamily(output_dir=tmp_path).build(tmp_path)
    # The other attribute tables still publish with receipts.
    assert {"docket_attributes", "document_attributes"}.isdisjoint(retired_receipt_datasets())
