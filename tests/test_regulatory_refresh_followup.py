"""Small runtime witnesses for the deferred regulatory refresh questions."""

import hashlib
from json import dumps

import polars as pl
import pytest
from spicy_docs.schemas.regulations import DOCKET
from spicy_docs.sources.mirrulations import MirrulationsReader

from spicy_regs.pipelines import docket_gaps
from spicy_regs.pipelines.rollups.regulatory_base import DocketsFamily
from spicy_regs.sources import r2
from tests.test_docket_gaps import NOW, SERVED, _Detail
from tests.test_mirrulations_reader import (
    AGENCY,
    BUCKET,
    PREFIX,
    _docket_key,
    _docket_payload,
    _FakeObj,
    _FakeS3Resource,
)
from tests.test_regulatory_base_families import _dockets, _publish_source, _published_ids


def test_same_key_changed_body_is_not_refetched_by_append_only_manifest():
    """Characterize the current limitation; this does not qualify mutable refresh."""
    key = _docket_key("EPA-2024-0001")
    original = _docket_payload("EPA-2024-0001")
    store = {key: dumps(original).encode()}
    gets = []

    class VersionedObject(_FakeObj):
        def get(self):
            gets.append(self.key)
            return {**super().get(), "ETag": hashlib.sha256(self._content).hexdigest(),
                    "ContentLength": len(self._content)}

    class VersionedSource(_FakeS3Resource):
        def Object(self, name, key):
            return VersionedObject(key, self._store[key])

    source = VersionedSource(store)
    first = MirrulationsReader(source, BUCKET, PREFIX, AGENCY, DOCKET)
    [first_record] = list(first.iter_keyed_records())
    assert first_record.payload == original
    changed = _docket_payload("EPA-2024-0001")
    changed["data"]["attributes"]["title"] = "Changed at the same object path"
    store[key] = dumps(changed).encode()
    second = MirrulationsReader(source, BUCKET, PREFIX, AGENCY, DOCKET, processed_keys=set(first.last_keys))
    assert list(second.iter_records()) == []
    assert gets == [key]
    # Explicit bounded reread sees the replacement. No real comments or mirror
    # bodies are fetched, and no historical identity is inferred.
    reread = MirrulationsReader(source, BUCKET, PREFIX, AGENCY, DOCKET)
    [changed_record] = list(reread.iter_keyed_records())
    assert changed_record.payload == changed and changed_record.etag != first_record.etag


@pytest.fixture
def gap_case(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install

    remote = install(monkeypatch)
    _publish_source(tmp_path, ["EPA-1"])
    working = tmp_path / "working"
    working.mkdir()
    _dockets(["EPA-1"]).write_parquet(working / "dockets.parquet")
    pl.DataFrame({"docket_id": [SERVED["data"]["id"]]}).write_parquet(working / "documents.parquet")
    pl.DataFrame({"docket_id": []}, schema={"docket_id": pl.String}).write_parquet(working / "comments_index.parquet")

    class Reader:
        def docket(self, identity):
            assert identity == SERVED["data"]["id"]
            return _Detail(SERVED)

    monkeypatch.setattr(r2, "preflight_uploads", lambda *args: None)
    monkeypatch.setattr(r2, "upload_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(r2, "download_working_copy", lambda *args: False)
    return remote, working, Reader()


def test_served_gap_docket_reaches_the_selected_native_publication(tmp_path, gap_case):
    remote, working, reader = gap_case
    result = docket_gaps.fill(working, reader, skip_upload=False, now=lambda: NOW)
    assert result["served"] == 1
    assert SERVED["data"]["id"] in pl.read_parquet(working / "dockets.parquet")["docket_id"].to_list()
    DocketsFamily(output_dir=tmp_path / "republish", skip_upload=False).run()
    assert set(_published_ids(remote)) == {"EPA-1", SERVED["data"]["id"]}


def test_gap_publication_conflict_preserves_the_current_native_selection(tmp_path, monkeypatch, gap_case):
    from spicy_regs.sources import publication

    remote, working, reader = gap_case
    expected = {}
    merge = docket_gaps.merge_staging_files

    def intervening(*args, **kwargs):
        merge(*args, **kwargs)
        _publish_source(tmp_path, ["EPA-1", "EPA-2"])
        expected["index"] = remote.objects[publication.INDEX_V2_KEY]

    monkeypatch.setattr(docket_gaps, "merge_staging_files", intervening)
    with pytest.raises((ValueError, RuntimeError), match="changed|concurrent|stale|Conflict"):
        docket_gaps.fill(working, reader, skip_upload=False, now=lambda: NOW)
    assert remote.objects[publication.INDEX_V2_KEY] == expected["index"]
    assert set(_published_ids(remote)) == {"EPA-1", "EPA-2"}
    assert not (working / ".native-state" / "selection.json").exists()
    assert not (working / docket_gaps.OUTCOMES).exists()
