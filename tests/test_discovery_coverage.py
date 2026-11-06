"""Native membership, selected-generation cursors and persistent retry work."""
from contextlib import contextmanager
from datetime import UTC, date, datetime
import importlib
import json

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.crs_reports import CrsReportsRollup
from spicy_regs.sources import publication as pub, r2
from spicy_regs.source_evidence import CaptureEvidence
from tests.generation_fakes import Store
from tests.test_crs_reports import _RAW_REPORT
from tests.test_print_citations import (
    _Reader, _Acquirer, _build, _listing, _package, _publish_as_prior,
    REPORT_PAGES, CRPT_ID, CHECKPOINT_NAMESPACE, read_checkpoints,
)


def journal(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


class Evidence:
    def __init__(self, state=None):
        self.state = state
        self.events = []

    def inherited_event(self, event, **match):
        return self.state

    def event(self, event, **fields):
        self.events.append({"event": event, **fields})


def test_crs_cursor_ignores_historical_rows_and_explicit_imports(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(r2, "download", lambda *_: False)
    windows = []

    class Reader:
        def __init__(self, *, since, evidence):
            windows.append(since)

        def iter_records(self):
            return iter([_RAW_REPORT])

    monkeypatch.setattr(module, "CrsReportsReader", Reader)
    evidence = CaptureEvidence(tmp_path, "crs-reports")
    monkeypatch.setattr(evidence, "inherited_event", lambda *_: {"cursor": "2026-11-05T13:00:00+00:00"})
    module.build_crs_reports(tmp_path, evidence=evidence, receipt_generation_id="initial")
    assert windows == [date(2026, 11, 2)]
    assert [e for e in journal(evidence) if e["event"] == "crs-discovery"][-1]["mode"] == "refresh"
    assert datetime.fromisoformat([e for e in journal(evidence) if e["event"] == "crs-discovery"][-1]["cursor"]) > datetime(2026, 11, 5, tzinfo=UTC)
    (tmp_path / module.OUTPUT).rename(tmp_path / "_crs_prior.parquet")
    module.build_crs_reports(tmp_path, since=date(2020, 1, 1), evidence=evidence, receipt_generation_id="archive")
    assert windows[-1] == date(2020, 1, 1)
    assert [e for e in journal(evidence) if e["event"] == "crs-discovery"][-1]["cursor"] == "2026-11-05T13:00:00+00:00"
    assert [e for e in journal(evidence) if e["event"] == "crs-discovery"][-1]["mode"] == "explicit-window"


@pytest.mark.parametrize("failure", [RuntimeError("failed page"), KeyboardInterrupt()])
def test_crs_failed_discovery_never_commits_a_cursor(tmp_path, monkeypatch, failure):
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(r2, "download", lambda *_: False)

    class Reader:
        def __init__(self, **kwargs):
            pass

        def iter_records(self):
            yield _RAW_REPORT
            raise failure

    monkeypatch.setattr(module, "CrsReportsReader", Reader)
    evidence = Evidence({"cursor": "2026-11-05T13:00:00+00:00"})
    with pytest.raises(type(failure)):
        module.build_crs_reports(tmp_path, evidence=evidence)
    assert evidence.events == []
    assert not (tmp_path / module.OUTPUT).exists()


def test_committee_cursor_comes_from_selected_discovery(monkeypatch):
    from spicy_regs.transforms.build_committee_reports import _since

    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    evidence = Evidence({"cursor": "2026-11-05T13:00:00+00:00"})
    assert _since(evidence=evidence) == "2026-11-04T13:00:00Z"


@pytest.mark.parametrize("already_held", [False, True])
def test_print_cap_persists_unattempted_work_even_when_next_inventory_omits_it(tmp_path, already_held):
    # No body read before the cap; next run's discovery does not list the package.
    package = _package(CRPT_ID, pages=REPORT_PAGES)
    acquirer = _Acquirer({CRPT_ID: package})
    coverage = {}
    modified = "2026-09-18T12:00:00Z"
    if already_held:
        initial = _build(tmp_path, _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT")]}), acquirer)
        _publish_as_prior(initial)
        acquirer.asked.clear()
        modified = "2026-09-19T12:00:00Z"
        acquirer.packages[CRPT_ID] = _package(CRPT_ID, pages=REPORT_PAGES, last_modified=modified)
    paths = _build(tmp_path, _Reader({"CRPT": [_listing(CRPT_ID, "ACTIVITY REPORT", modified)]}),
                   acquirer, max_packages=0, coverage=coverage)
    assert acquirer.asked == [] and coverage["deferred"] == 1
    states = read_checkpoints(paths[0], CHECKPOINT_NAMESPACE)
    assert states[0]["package_id"] == CRPT_ID
    assert states[0]["pending_last_modified"] == modified
    _publish_as_prior(paths)
    paths = _build(tmp_path, _Reader({}), acquirer, coverage=coverage)
    assert acquirer.asked == [CRPT_ID]
    assert pq.read_table(paths[0]).num_rows == (2 if already_held else 1)
    assert coverage["refused"] == 0 and coverage["deferred"] == 0


def test_maintained_crs_entrypoint_uses_only_selected_evidence_after_conversion_failure(tmp_path, monkeypatch):
    """A later completed crawl in failed generation assembly never becomes progress.

    The normal rollup builds actual native tables/receipts, generation, evidence,
    and publication index in the in-memory object store. No network is available.
    """
    from spicy_regs import generations
    from spicy_regs.sources import cloudflare

    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    store = Store()
    windows = []
    fail = False
    original = generations.build_generation

    class Reader:
        def __init__(self, *, since, evidence):
            windows.append(since)

        def iter_records(self):
            return iter([_RAW_REPORT])

    def generation(*args, **kwargs):
        if fail:
            raise RuntimeError("injected after crawl before generation commit")
        return original(*args, **kwargs)

    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test.invalid/")
        raw = store.objects.get(key)
        yield httpx.Response(404 if raw is None else 200, content=raw or b"", request=httpx.Request(method, url))

    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.delenv("CRS_REPORTS_RECONCILE", raising=False)
    monkeypatch.delenv("CRS_REPORTS_SINCE", raising=False)
    monkeypatch.setattr(module, "CrsReportsReader", Reader)
    monkeypatch.setattr(generations, "build_generation", generation)
    monkeypatch.setattr(pub.httpx, "stream", stream)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(cloudflare, "purge_urls", lambda _: None)
    # Each entrypoint reads the actual selected index, including its evidence pins.
    class Frozen(datetime):
        day = 5

        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, cls.day, 13, tzinfo=UTC)

    monkeypatch.setattr(module, "datetime", Frozen)
    for run in range(3):
        Frozen.day = [5, 20, 21][run]
        pipeline = CrsReportsRollup(output_dir=tmp_path / str(run), skip_upload=False)
        if run == 1:
            selected_before = store.objects[pub.INDEX_V2_KEY]
            fail = True
            with pytest.raises(RuntimeError, match="after crawl"):
                pipeline.run()
            assert store.objects[pub.INDEX_V2_KEY] == selected_before
            assert any(e["event"] == "crs-discovery" for e in journal(pipeline.source_evidence))
        else:
            fail = False
            pipeline.run()
            assert pipeline.acquisition_status["discovery"] == "complete"
    assert windows[0] is None
    assert windows[1] == windows[2] == date(2026, 11, 2)


def test_committee_reconciliation_names_both_native_bounds(tmp_path):
    from spicy_regs.transforms.build_committee_reports import _package_ids

    asked = []

    class Reader:
        def packages(self, url, *, max_pages):
            asked.append(url)
            return iter([])

    assert _package_ids(Reader(), "BUDGET", "2026-01-01T00:00:00Z", until="2026-02-01T00:00:00Z") == {}
    assert "/2026-01-01T00:00:00Z/2026-02-01T00:00:00Z?" in asked[0]


@pytest.mark.parametrize("field,value", [("title", "Conflicting source title"), ("version", 999),
                                         ("status", "Contradictory"), ("publishDate", "2026-01-01T00:00:00Z")])
def test_list_only_same_revision_conflict_preserves_prior_bytes(tmp_path, monkeypatch, field, value):
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(r2, "download", lambda *_: False)
    incoming = [dict(_RAW_REPORT)]
    monkeypatch.setattr(module.CrsReportsReader, "iter_records", lambda _: iter(incoming))
    path = module.build_crs_reports(tmp_path, receipt_generation_id="list-initial")
    original = path.read_bytes()
    path.rename(tmp_path / "_crs_prior.parquet")
    incoming[0] = {**_RAW_REPORT, field: value}
    with pytest.raises(GovernmentShapeError, match="incompatible same-coordinate list metadata"):
        module.build_crs_reports(tmp_path, receipt_generation_id="list-conflict")
    assert (tmp_path / "_crs_prior.parquet").read_bytes() == original
    assert not path.exists()


@pytest.mark.parametrize("presence", ["missing", "null", "empty"])
def test_same_revision_list_absence_and_explicit_clearing_stay_distinct(tmp_path, monkeypatch, presence):
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(r2, "download", lambda *_: False)
    incoming = [dict(_RAW_REPORT)]
    monkeypatch.setattr(module.CrsReportsReader, "iter_records", lambda _: iter(incoming))
    path = module.build_crs_reports(tmp_path, receipt_generation_id="list-initial")
    path.rename(tmp_path / "_crs_prior.parquet")
    if presence == "missing":
        del incoming[0]["title"]
    else:
        incoming[0]["title"] = None if presence == "null" else ""
    path = module.build_crs_reports(tmp_path, receipt_generation_id="list-clearing")
    assert pq.read_table(path)["title"].to_pylist() == [_RAW_REPORT["title"] if presence == "missing" else incoming[0]["title"]]


@pytest.mark.parametrize("field,value", [("title", "Other populated title"), ("version", 999)])
def test_fresh_crs_pages_refuse_same_coordinate_conflicts_before_pooling(tmp_path, field, value):
    from spicy_regs.sources.crs_reports import CrsReportsReader
    from spicy_regs.transforms.government_source_shapes import GovernmentShapeError

    raw = json.dumps({"CRSReports": [_RAW_REPORT, {**_RAW_REPORT, field: value}],
                      "pagination": {"count": 2}}).encode()
    evidence = CaptureEvidence(tmp_path, "crs-reports")
    reader = CrsReportsReader(api_key="offline", evidence=evidence, transport=httpx.MockTransport(
        lambda _: httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"})))
    with pytest.raises(GovernmentShapeError, match="incompatible same-coordinate"):
        list(reader.iter_records())
    captures = [row for row in journal(evidence) if row["event"] == "capture"]
    assert len(captures) == 1 and captures[0]["byte_size"] == len(raw)


def test_fresh_crs_pool_retains_each_observed_dated_revision():
    from spicy_regs.sources.crs_reports import CrsReportsReader

    later = {**_RAW_REPORT, "updateDate": "2026-07-18T01:00:00Z", "title": "Later source title", "version": 16}
    pages = iter([[_RAW_REPORT, _RAW_REPORT], [later, {**_RAW_REPORT, "id": "R2"}]])
    def serve(_):
        raw = json.dumps({"CRSReports": next(pages), "pagination": {"count": 2}}).encode()
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"})
    rows = list(CrsReportsReader(api_key="offline", transport=httpx.MockTransport(serve)).iter_records())
    assert {(row["id"], row["updateDate"]) for row in rows} == {
        (_RAW_REPORT["id"], _RAW_REPORT["updateDate"]), (_RAW_REPORT["id"], later["updateDate"]),
        ("R2", _RAW_REPORT["updateDate"]),
    }
