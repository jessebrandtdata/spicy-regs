"""The scheduled FR row writer and restore fallbacks explain their work."""

import shutil

from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, regulations_bulk
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup
from spicy_regs.transforms.regulations_receipts import materialize_internal
from tests.test_federal_register_bulk_restore import _source as register_source
from tests.test_fr_docket_links_bulk_restore import _source as links_source


@pytest.fixture(params=["federal_register", "fr_docket_links"])
def held(tmp_path, request):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    if request.param == "federal_register":
        return register_source(source_dir, [
            {"document_number": "2026-00001", "publication_date": "2026-06-30"},
            {"document_number": "2026-00002", "publication_date": "2026-06-30"},
        ])
    return links_source(source_dir)


@pytest.fixture
def routes():
    messages = []
    sink = logger.add(lambda message: messages.append(message.record["message"]),
                      filter=lambda record: "route=row" in record["message"])
    yield messages
    logger.remove(sink)


def test_scheduled_reference_writer_logs_reason_and_source_rows(tmp_path, monkeypatch, held, routes):
    selected, source, _ = held
    dataset = selected.dataset
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    class LocalRollup(SubjectReceiptRollup):
        name = "row-route-test"
        output = dataset + ".parquet"

        def build(self, output_dir):
            raise NotImplementedError

    def build(directory):
        target = directory / (dataset + ".parquet")
        shutil.copyfile(source, target)
        return target

    output = tmp_path / "output"
    output.mkdir()
    result = LocalRollup().build_receipts(output, build)
    assert pq.ParquetFile(result).metadata.num_rows == pq.ParquetFile(source).metadata.num_rows
    assert routes == [
        f"regulatory write route=row dataset={dataset} rows={pq.ParquetFile(source).metadata.num_rows} "
        "reason=bulk not requested by subject receipt rollup"
    ]


def test_batch_ineligibility_logs_rows_and_keeps_exact_reference_output(tmp_path, monkeypatch, held, routes):
    selected, source, schema = held

    def unproven(*args, **kwargs):
        raise etl_bulk.NotBulkEligible("bounded batch proof unavailable")

    monkeypatch.setattr(regulations_bulk, "_materialize_selected", unproven)
    restored = materialize_internal(selected, tmp_path / "restored.parquet", bulk=True, source_schema=schema)
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)
    assert routes == [
        f"regulatory restore route=row dataset={selected.dataset} rows={pq.ParquetFile(selected.subjects[0]).metadata.num_rows} "
        "reason=bounded batch proof unavailable"
    ]


def test_schema_mismatch_logs_rows_before_refusal_and_preserves_destination(tmp_path, held, routes):
    selected, _, _ = held
    subject = pq.read_table(selected.subjects[0])
    pq.write_table(subject.append_column("undeclared", pa.nulls(subject.num_rows, pa.string())), selected.subjects[0])
    destination = tmp_path / "preserved.parquet"
    destination.write_bytes(b"preserved")
    with pytest.raises(ValueError, match="Subject schema differs from policy"):
        materialize_internal(selected, destination, bulk=True)
    assert destination.read_bytes() == b"preserved"
    assert routes == [
        f"regulatory restore route=row dataset={selected.dataset} rows={subject.num_rows} reason=subject-schema-differs"
    ]


def test_qualified_batch_restore_stays_bulk_without_row_warning(tmp_path, held, routes):
    selected, source, schema = held
    restored = materialize_internal(selected, tmp_path / "restored.parquet", bulk=True, source_schema=schema)
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)
    assert routes == []
