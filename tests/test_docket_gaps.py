"""Dockets the mirror never captured are asked of the publisher once, and every answer is kept."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.regulations_gov.api import RegulationsGovApiUnavailableError
from spicy_docs.transport.captured import CapturedBodyResponse, attach_capture

from spicy_regs.pipelines import docket_gaps
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import r2

NOW = datetime(2026, 9, 26, 12, tzinfo=UTC)
ATTRIBUTES = {"agencyId": "BIS", "title": "Additions to the Entity List", "docketType": "Rulemaking",
              "modifyDate": "2023-03-29T14:51:26Z", "dkAbstract": None, "rin": "Not Assigned"}
SERVED = {"data": {"id": "BIS-2023-0005", "type": "dockets", "links": {}, "attributes": ATTRIBUTES}}


def _capture(status: int, body: bytes) -> CapturedBodyResponse:
    return CapturedBodyResponse("u", "u", status, "application/vnd.api+json", "2026-09-26T12:00:00Z", body)


class _Detail:
    def __init__(self, payload):
        self.data = payload["data"]
        self.capture = _capture(200, json.dumps(payload).encode())


class _Reader:
    """Answers like the publisher did on 2026-09-26, plus one transport failure."""

    def __init__(self):
        self.asked = []

    def docket(self, docket_id):
        self.asked.append(docket_id)
        if docket_id == "BIS-2023-0005":
            return _Detail(SERVED)
        if docket_id == "CFPB-2026-0008":
            raise RegulationsGovApiUnavailableError(_capture(404, b'{"errors":[{"status":"404"}]}'))
        if docket_id == "GIPSA-2010-FGIS-0004":
            return _Detail({"data": {"id": docket_id, "type": "dockets", "links": {},
                                     "attributes": {**ATTRIBUTES, "agencyId": "GIPSA"}}})
        if docket_id.endswith("-RULEMAKING"):
            error = PagedJsonSourceError("Body source answered HTTP 400")
            attach_capture(error, _capture(400, b'{"errors":[{"status":"400","title":"Invalid ID: ' + docket_id.encode() + b'"}]}'))
            raise error
        raise PagedJsonSourceError("connection reset")


def _working_copies(directory: Path) -> None:
    dockets = RECORD_TYPES["dockets"]
    pl.DataFrame([{**dict.fromkeys(dockets.schema), "docket_id": "EPA-1", "agency_code": "EPA"}],
                 schema=dockets.schema).write_parquet(directory / "dockets.parquet")
    pl.DataFrame({"docket_id": ["EPA-1", "BIS-2023-0005", "CFPB-2026-0008", None]}).write_parquet(
        directory / "documents.parquet")
    pl.DataFrame({"docket_id": ["GIPSA-2010-FGIS-0002-RULEMAKING", "GIPSA-2010-FGIS-0004-RULEMAKING",
                                "FAA-2026-9999", "EPA-1"]}).write_parquet(
        directory / "comments_index.parquet")


@pytest.fixture
def working(tmp_path, monkeypatch):
    _working_copies(tmp_path)
    monkeypatch.setattr(r2, "download_working_copy", lambda key, path: False)
    return tmp_path


def test_each_answer_is_classified_and_recorded(working):
    reader = _Reader()
    with pytest.raises(RuntimeError, match="1 docket request"):
        docket_gaps.fill(working, reader, now=lambda: NOW)
    assert reader.asked == ["BIS-2023-0005", "CFPB-2026-0008", "FAA-2026-9999", "GIPSA-2010-FGIS-0002-RULEMAKING",
                            "GIPSA-2010-FGIS-0002", "GIPSA-2010-FGIS-0004-RULEMAKING", "GIPSA-2010-FGIS-0004"]
    outcomes = pl.read_parquet(working / docket_gaps.OUTCOMES).sort("docket_id")
    assert dict(zip(outcomes["docket_id"], outcomes["outcome"], strict=True)) == {
        "BIS-2023-0005": "served", "CFPB-2026-0008": "absent", "GIPSA-2010-FGIS-0002-RULEMAKING": "invalid",
        "GIPSA-2010-FGIS-0004-RULEMAKING": "alias"}
    aliases = outcomes.filter(pl.col("outcome") == "alias")
    assert aliases["canonical_id"].to_list() == ["GIPSA-2010-FGIS-0004"]
    assert dict(zip(outcomes["docket_id"], outcomes["http_status"], strict=True))["CFPB-2026-0008"] == 404


def test_absent_and_invalid_ids_are_not_asked_again_until_the_retry_window_passes(working):
    with pytest.raises(RuntimeError):
        docket_gaps.fill(working, _Reader(), now=lambda: NOW)
    reader = _Reader()
    with pytest.raises(RuntimeError):
        docket_gaps.fill(working, reader, now=lambda: NOW + timedelta(days=1))
    assert reader.asked == ["BIS-2023-0005", "FAA-2026-9999", "GIPSA-2010-FGIS-0004-RULEMAKING", "GIPSA-2010-FGIS-0004"]
    later = _Reader()
    with pytest.raises(RuntimeError):
        docket_gaps.fill(working, later, now=lambda: NOW + docket_gaps.RETRY_AFTER + timedelta(days=1))
    assert "CFPB-2026-0008" in later.asked and "GIPSA-2010-FGIS-0002-RULEMAKING" in later.asked


def test_a_served_docket_becomes_a_row_through_the_etl_extraction():
    rows = list(docket_gaps.ExtractRecords(docket_gaps.DOCKETS).apply([SERVED]))
    assert rows == [{"docket_id": "BIS-2023-0005", "agency_code": "BIS", "title": "Additions to the Entity List",
                     "docket_type": "Rulemaking", "modify_date": "2023-03-29T14:51:26Z", "abstract": None,
                     "rin": "Not Assigned"}]


def test_a_served_docket_is_merged_over_the_selected_dockets_and_the_outcomes_uploaded(working, monkeypatch):
    from tests.regulatory_publication_fakes import install

    install(monkeypatch)
    uploaded, finalized = [], []

    def restore(root, dataset, destination, *, inputs):
        destination.parent.mkdir(parents=True)
        pl.read_parquet(working / "dockets.parquet").write_parquet(destination)
        return True

    monkeypatch.setattr(docket_gaps, "restore_dataset", restore)
    monkeypatch.setattr(docket_gaps, "finish_dataset", lambda root, dataset, source, **kw:
                        finalized.append((root, dataset, pl.read_parquet(source)["docket_id"].sort().to_list(), kw)))
    monkeypatch.setattr(r2, "upload_file", lambda path, remote_key=None: uploaded.append(remote_key))
    with pytest.raises(RuntimeError, match="1 docket request"):
        docket_gaps.fill(working, _Reader(), skip_upload=False, now=lambda: NOW)
    assert uploaded == [docket_gaps.OUTCOMES]
    [(root, dataset, ids, options)] = finalized
    assert (root, dataset, ids) == (working, "dockets", ["BIS-2023-0005", "EPA-1", "GIPSA-2010-FGIS-0004"])
    assert options["publish"] is True and options["inputs"].index is not None


def test_served_dockets_without_a_selected_generation_publish_nothing(working, monkeypatch):
    from tests.regulatory_publication_fakes import install

    install(monkeypatch)
    monkeypatch.setattr(docket_gaps, "finish_dataset", lambda *a, **k: pytest.fail("published without the prior"))
    with pytest.raises(RuntimeError, match="no selected dockets generation"):
        docket_gaps.fill(working, _Reader(), skip_upload=False, now=lambda: NOW)
    assert not (working / docket_gaps.OUTCOMES).exists()
