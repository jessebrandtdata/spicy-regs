"""The maintained CRS list/detail merge, including consumer-visible recovery."""
from __future__ import annotations

import importlib
import json
import shutil

import duckdb
import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.government_receipts import internal_prior, POLICIES
from spicy_regs.transforms.government_source_shapes import GovernmentShapeError
from spicy_regs.etl_receipts import validate_receipt_bundle

LIST = {"id": "R1", "updateDate": "2026-10-01T00:00:00Z", "version": 2,
        "publishDate": "2026-09-30T00:00:00Z", "title": "Report", "contentType": "Reports", "status": "Active"}
DETAIL = {"id": "R1", "updateDate": LIST["updateDate"], "currentVersion": 2, "title": "Report",
          "summary": "Source summary", "authors": [{"author": "A"}, {"author": "A"}], "formats": [],
          "relatedMaterials": [{"URL": "https://api.congress.gov/v3/law/119/pub/21", "congress": 119,
                                "number": "119-21", "title": None, "type": "PUB"}] * 2,
          "unfamiliar": {"preserved": True}}


def setup_builder(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    incoming, asked = [dict(LIST)], []
    monkeypatch.setattr(module.CrsReportsReader, "iter_records", lambda self: iter(incoming))
    response = {"status": 200, "report": dict(DETAIL)}
    def handler(request):
        asked.append(request)
        return httpx.Response(response["status"], json={"CRSReport": response["report"]})
    def build(*, limit=1, retry=False):
        output = tmp_path / module.OUTPUT
        if output.exists():
            shutil.copyfile(output, tmp_path / "_crs_prior.parquet")
        return module.build_crs_reports(tmp_path, detail_limit=limit, detail_api_key="test-key",
                                        detail_transport=httpx.MockTransport(handler), retry_completed=retry)
    return build, incoming, response, asked


def literal(path):
    return pq.read_table(internal_prior("crs_reports", path)).to_pylist()


def test_qualified_detail_survives_refresh_failed_retry_and_explicit_clearing(tmp_path, monkeypatch):
    build, incoming, response, asked = setup_builder(tmp_path, monkeypatch)
    path = build()
    [qualified] = literal(path)
    assert qualified["detail_state"] == qualified["detail_attempt_state"] == "ok"
    assert "unfamiliar" not in qualified  # The pinned exact original keeps unfamiliar fields.
    assert json.loads(qualified["detail_fields_json"])[2] == {"name": "topics", "state": "missing"}
    assert len(asked) == 1
    path = build(limit=0)  # Maintained ordinary list-only refresh at the same coordinate.
    assert literal(path) == [qualified]
    path = build()  # Unchanged successful revision needs no second detail request.
    assert len(asked) == 1
    response["status"] = 404
    path = build(retry=True)
    [failed] = literal(path)
    assert failed["detail_attempt_state"] == "failed" and failed["detail_state"] == "ok"
    for field in ("summary", "detail_fields_json", "detail_witness_json"):
        assert failed[field] == qualified[field]
    response["status"] = 200
    path = build()  # Failed latest attempt retries, even with an earlier completion.
    assert len(asked) == 3 and literal(path)[0]["detail_attempt_state"] == "ok"
    response["report"] = {**DETAIL, "summary": None, "authors": [], "formats": None}
    path = build(retry=True)
    [cleared] = literal(path)
    assert cleared["summary"] is None and json.loads(cleared["authors_json"]) == []
    assert json.loads(cleared["formats_json"]) is None
    with duckdb.connect() as con:
        row = con.execute("SELECT summary, authors, topics, formats, len(related_materials) FROM read_parquet(?)", [str(path)]).fetchone()
        assert row == (None, [], None, None, 2)
    validate_receipt_bundle({"crs_reports": [path]}, [tmp_path / "etl_receipts.parquet"],
                            [POLICIES["crs_reports"]])


@pytest.mark.parametrize("change", ["race", "version", "id", "content", "missing", "list"])
def test_incompatible_native_answer_keeps_prior_and_original(tmp_path, monkeypatch, change):
    build, incoming, response, asked = setup_builder(tmp_path, monkeypatch)
    path = build()
    before = path.read_bytes()
    if change == "race":
        response["report"]["updateDate"] = "2026-10-02T00:00:00Z"
    elif change == "version":
        response["report"]["currentVersion"] = 3
    elif change == "id":
        response["report"]["id"] = "R2"
    elif change == "content":
        response["report"]["summary"] = "Incompatible populated content"
    elif change == "missing":
        response["report"].pop("summary")
    else:
        incoming[0]["version"] = 3
    with pytest.raises(GovernmentShapeError, match="prior preserved|same-coordinate"):
        build(retry=True)
    assert path.read_bytes() == before
    # Full original JSON survives even when source identity/coordinate is refused.
    if change != "list":
        raw = json.dumps({"CRSReport": response["report"]}, separators=(",", ":")).encode()
        import hashlib
        digest = hashlib.sha256(raw).hexdigest()
        assert (tmp_path / "raw/objects/sha256" / digest[:2] / digest).read_bytes() == raw


def test_initial_detail_with_contradictory_shared_metadata_is_refused(tmp_path, monkeypatch):
    build, incoming, response, asked = setup_builder(tmp_path, monkeypatch)
    response["report"]["title"] = "A different report title at the same coordinate"
    with pytest.raises(GovernmentShapeError, match="incompatible list/detail metadata"):
        build()
    assert not (tmp_path / "crs_reports.parquet").exists()


def test_later_revision_enriches_without_changing_older_rows_and_preserves_multiplicity(tmp_path, monkeypatch):
    build, incoming, response, asked = setup_builder(tmp_path, monkeypatch)
    path = build()
    old = literal(path)[0]
    incoming[0] = {**LIST, "updateDate": "2026-10-02T00:00:00Z", "version": 3}
    response["report"] = {**DETAIL, "updateDate": incoming[0]["updateDate"], "currentVersion": 3}
    path = build()
    assert len(asked) == 2 and literal(path)[1] == old
    with duckdb.connect() as con:
        assert con.execute("SELECT summary, authors[1].author, len(authors), len(related_materials) "
                           "FROM read_parquet(?) ORDER BY update_date", [str(path)]).fetchall() == [
                               ("Source summary", "A", 2, 2), ("Source summary", "A", 2, 2)]
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone() == (2,)
