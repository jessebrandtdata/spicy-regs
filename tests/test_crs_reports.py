"""CRS revisions, field mapping and credential resolution."""

from __future__ import annotations

import importlib
import shutil
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.crs_reports import (
    API_KEY_ENV_VARS,
    _resolve_api_key,
)
from spicy_regs.transforms.build_crs_reports import COLUMNS, _shape

_RAW_REPORT = {
    "contentType": "Reports",
    "id": "R48641",
    "publishDate": "2026-07-16T04:00:00Z",
    "status": "Active",
    "title": "Proposals to Limit Member of Congress Financial Activities",
    "updateDate": "2026-07-17T22:38:55Z",
    "url": "https://api.congress.gov/v3/crsreport/R48641",
    "version": 15,
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_REPORT)
    assert set(row) == set(COLUMNS)
    assert "summary" in COLUMNS and "detail_state" in COLUMNS


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_REPORT)
    assert row["report_id"] == "R48641"
    assert row["title"] == "Proposals to Limit Member of Congress Financial Activities"
    assert row["report_type"] == "Reports"
    assert row["status"] == "Active"
    assert row["published_date"] == "2026-07-16T04:00:00Z"
    assert row["update_date"] == "2026-07-17T22:38:55Z"
    # version int is stringified.
    assert row["version"] == "15"
    assert row["url"] == "https://api.congress.gov/v3/crsreport/R48641"


def test_shape_handles_missing_scalars():
    row = _shape({"id": "IN12713"})
    assert row["report_id"] == "IN12713"
    assert row["title"] is None
    assert row["report_type"] is None
    assert row["version"] is None


@pytest.mark.parametrize("bulk", [False, True])
def test_build_retains_revisions_and_repeated_imports_deduplicate(tmp_path, monkeypatch, bulk):
    from spicy_regs import etl_bulk, government_bulk
    from spicy_regs.etl_receipts import validate_receipt_bundle
    from spicy_regs.transforms.government_receipts import POLICIES, internal_prior

    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    if not bulk:
        def row_path(*args, **kwargs):
            raise etl_bulk.NotBulkEligible("Exercise the reference writer and reader")
        monkeypatch.setattr(government_bulk, "write_dataset", row_path)
        monkeypatch.setattr(government_bulk, "restore_legacy", row_path)
    incoming, windows = [], []

    class Reader:
        def __init__(self, *, since, evidence):
            windows.append(since)

        def iter_records(self):
            return iter(incoming)

    monkeypatch.setattr(module, "CrsReportsReader", Reader)
    original = dict(_RAW_REPORT)
    # A metadata revision can keep the report version and publication date.
    revised = {**original, "updateDate": "2026-07-17T23:59:59Z", "title": "Revised title"}
    new_edition = {**revised, "updateDate": "2026-07-20T12:00:00Z",
                   "publishDate": "2026-07-20T04:00:00Z", "version": 16}
    corrected = dict(new_edition)  # repeated observations must agree at the same revision
    output = tmp_path / module.OUTPUT
    for index, reports in enumerate(([original], [revised], [new_edition], [corrected, corrected], [], [original])):
        if output.exists():
            shutil.copyfile(output, tmp_path / "_crs_prior.parquet")
        incoming[:] = reports
        module.build_crs_reports(tmp_path, receipt_generation_id=f"run-{index}")
        expected = min(index + 1, 3)
        assert pq.ParquetFile(output).metadata.num_rows == expected
        validate_receipt_bundle(
            {"crs_reports": [output]}, [tmp_path / "etl_receipts.parquet"],
            [POLICIES["crs_reports"]], generation_id=f"run-{index}",
        )
    restored = pq.read_table(internal_prior("crs_reports", output)).to_pylist()
    assert {row["update_date"]: row for row in restored} == {
        raw["updateDate"]: _shape(raw) for raw in (original, revised, corrected)
    }
    with duckdb.connect() as con:
        assert con.execute(
            "SELECT report_id, min(published_date), count(*) FROM read_parquet(?) GROUP BY report_id", [str(output)],
        ).fetchall() == [(original["id"], datetime(2026, 7, 16, 4, tzinfo=timezone.utc), 3)]
    assert windows[0] is None
    assert windows[-1] is None  # imported rows alone do not establish a live discovery cursor


@pytest.mark.parametrize("bulk", [False, True])
def test_existing_report_key_migrates_without_losing_its_row(tmp_path, monkeypatch, bulk):
    from spicy_regs import etl_bulk, government_bulk
    from spicy_regs.etl_receipts import (
        decode_exact_json, receipt_policies, resolve_receipt_witness, subject_identity, validate_receipt_bundle,
    )
    from spicy_regs.transforms import government_receipts as gr

    if not bulk:
        def row_path(*args, **kwargs):
            raise etl_bulk.NotBulkEligible("Exercise the reference migration writer and reader")
        monkeypatch.setattr(government_bulk, "write_dataset", row_path)
        monkeypatch.setattr(government_bulk, "restore_legacy", row_path)
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    current, *history = receipt_policies(gr.POLICIES["crs_reports"])
    earlier = history[-1]
    prior = tmp_path / "_crs_prior.parquet"
    pq.write_table(pa.Table.from_pylist([_shape(_RAW_REPORT)], schema=module._SCHEMA), prior)
    with monkeypatch.context() as patch:
        patch.setitem(gr.POLICIES, "crs_reports", earlier)
        current_mapper = gr.map_subject
        patch.setattr(gr, "map_subject", lambda dataset, raw, **kwargs: {
            k: v for k, v in current_mapper(dataset, raw, **kwargs).items() if k in earlier.subject_schema.names
        })
        gr.migrate_outputs((prior,), datasets=("crs_reports",), generation_id="old-policy")
    before = prior.read_bytes()
    [old] = pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist()
    revised = {**_RAW_REPORT, "updateDate": "2026-07-20T12:00:00Z", "version": 16}
    monkeypatch.setattr(module.CrsReportsReader, "iter_records", lambda self: iter([revised]))
    output = module.build_crs_reports(tmp_path, receipt_generation_id="new-policy")
    assert prior.read_bytes() == before
    assert pq.read_schema(output).equals(current.subject_schema)
    assert pq.read_table(output)["version"].to_pylist() == [16, 15]
    validate_receipt_bundle(
        {"crs_reports": [output]}, [tmp_path / "etl_receipts.parquet"], [current], generation_id="new-policy",
    )
    receipts = pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist()
    dated = {dict(decode_exact_json(receipt["identity_json"]))["update_date"]: receipt for receipt in receipts}
    subjects = pq.read_table(output).to_pylist()
    migrated = dated[subjects[1]["update_date"]]
    assert migrated["policy_version"] == current.policy_version
    assert migrated["record_id"] == subject_identity(current, subjects[1])[0] != old["record_id"]
    assert migrated["receipt_id"] != old["receipt_id"]
    assert migrated["processing_json"] == old["processing_json"]
    reference = decode_exact_json(migrated["diagnostic_json"])["prior_receipt"]
    assert {key: reference[key] for key in ("receipt_id", "generation_id", "policy_version", "record_id")} == {
        key: old[key] for key in ("receipt_id", "generation_id", "policy_version", "record_id")}
    assert all(witness in migrated["witnesses"] for witness in old["witnesses"])
    inherited = next(w for w in migrated["witnesses"] if w["source_id"] == "prior-etl-processing")
    assert resolve_receipt_witness(migrated, inherited) == old["processing_json"].encode()
    assert "prior_receipt" not in decode_exact_json(dated[subjects[0]["update_date"]]["diagnostic_json"])
    shutil.copyfile(output, prior)
    module.build_crs_reports(tmp_path, receipt_generation_id="repeated")
    assert pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist() == receipts
    validate_receipt_bundle(
        {"crs_reports": [output]}, [tmp_path / "etl_receipts.parquet"], [current], generation_id="repeated",
    )


@pytest.mark.parametrize("missing_column", [False, True])
def test_undated_prior_is_preserved_and_refused_before_fetch(tmp_path, monkeypatch, missing_column):
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    prior = tmp_path / "_crs_prior.parquet"
    table = pa.Table.from_pylist([{**_shape(_RAW_REPORT), "update_date": None}], schema=module._SCHEMA)
    pq.write_table(table.drop(["update_date"]) if missing_column else table, prior)
    output = tmp_path / module.OUTPUT
    output.write_bytes(b"existing output")
    before = prior.read_bytes(), output.read_bytes()

    def unexpected_fetch(*args, **kwargs):
        pytest.fail("An undated prior must be refused before fetching")

    monkeypatch.setattr(module, "CrsReportsReader", unexpected_fetch)
    with pytest.raises(GovernmentShapeError, match="prior revisions require update_date"):
        module.build_crs_reports(tmp_path)
    assert (prior.read_bytes(), output.read_bytes()) == before


def test_new_undated_report_is_refused_without_inventing_an_identity(tmp_path):
    from spicy_regs.etl_receipts import read_attempts
    from spicy_regs.transforms.government_receipts import POLICIES, migrate_outputs
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError
    from spicy_regs.transforms.build_crs_reports import _SCHEMA

    path = tmp_path / "crs_reports.parquet"
    raw = {**_shape(_RAW_REPORT), "update_date": None}
    pq.write_table(pa.Table.from_pylist([raw], schema=_SCHEMA), path)
    before = path.read_bytes()
    with pytest.raises(GovernmentShapeError, match="refused inputs"):
        migrate_outputs((path,), generation_id="undated")
    assert path.read_bytes() == before
    [attempt] = read_attempts(
        [tmp_path / ".government-etl/undated/etl_receipts.parquet"], POLICIES["crs_reports"], generation_id="undated",
    )
    assert attempt["outcome"] == "refused"
    assert attempt["processing_fields"]["raw_record"] == raw


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("CONGRESS_GOV_API_KEY", "congress-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None
