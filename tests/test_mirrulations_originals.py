"""The regulations ETL keeps every Mirrulations object it reads, exactly as received, before anything retires its key.

A fake S3 resource stands in for the public mirror and the in-memory ``Store``
for R2; the real reader, staging, evidence owner, central raw archive and
evidence publisher run.
"""

import hashlib
import json
from datetime import UTC, datetime
from json import dumps
from pathlib import Path

import polars as pl
import pytest

import spicy_docs.sources.mirrulations as mirrulations
import spicy_regs.pipelines.mirrulations_originals as originals_module
import spicy_regs.pipelines.regulations as regulations
from spicy_regs.pipelines import RegulationsPipeline
from spicy_regs.pipelines.mirrulations_originals import MirrulationsOriginals
from spicy_regs.source_evidence import SourceEvidenceError
from tests.regulatory_publication_fakes import install as install_publication
from tests.test_regulations_pipeline import (
    AGENCY,
    _comment_key,
    _comment_payload,
    _docket_key,
    _docket_payload,
)

MODIFIED = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)


@pytest.fixture
def r2_store(monkeypatch):
    return install_publication(monkeypatch)


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self, *_size) -> bytes:
        return self._data

    def close(self) -> None:
        pass


class _Object:
    def __init__(self, mirror: "_Mirror", key: str) -> None:
        self._mirror, self._key = mirror, key

    def get(self, **_options) -> dict:
        self._mirror.gets.append(self._key)
        content = self._mirror.objects[self._key]
        declared = self._mirror.declared.get(self._key, len(content))
        return {
            "Body": _Body(content),
            "ContentLength": declared,
            "ETag": '"' + hashlib.md5(content, usedforsecurity=False).hexdigest() + '"',
            "LastModified": MODIFIED,
            "VersionId": None,
        }


class _Listed:
    def __init__(self, key: str) -> None:
        self.key = key


class _Bucket:
    def __init__(self, mirror: "_Mirror") -> None:
        self.objects = self
        self._mirror = mirror

    def filter(self, Prefix: str):  # noqa: N803 - boto3 keyword
        return [_Listed(key) for key in sorted(self._mirror.objects) if key.startswith(Prefix)]


class _Mirror:
    """A public-mirror stand-in that records every GET and can declare a longer body than it sends."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.declared: dict[str, int] = {}
        self.gets: list[str] = []

    def Bucket(self, _name: str) -> _Bucket:  # noqa: N802 - boto3 API
        return _Bucket(self)

    def Object(self, _bucket: str, key: str) -> _Object:  # noqa: N802 - boto3 API
        return _Object(self, key)


def _dockets(*ids: str) -> dict[str, bytes]:
    return {_docket_key(i): dumps(_docket_payload(i, "2024-01-01")).encode() for i in ids}


def _run(output: Path, **options) -> None:
    settings = dict(allow_fresh_start=True, agency=AGENCY, output_dir=output, skip_comments=True, skip_upload=True)
    RegulationsPipeline(**{**settings, **options}).run()


def _raw_root() -> Path:
    import os

    return Path(os.environ["SPICYGOV_RAW_ROOT"])


def _retained_events(output: Path) -> list[dict]:
    events = []
    for journal in sorted((output / "source-evidence").glob("*/artifact/journal.jsonl")):
        events += [json.loads(line) for line in journal.read_text().splitlines()]
    return [event for event in events if event["event"] == "retained-file"]


def _archived(content: bytes) -> bytes:
    digest = hashlib.sha256(content).hexdigest()
    return (_raw_root() / "objects/sha256" / digest[:2] / digest).read_bytes()


def _manifest_keys(output: Path) -> set[str]:
    path = output / "manifest.parquet"
    return set(pl.read_parquet(path)["key"].to_list()) if path.exists() else set()


def test_every_read_object_is_retained_exactly_with_its_source_identity_and_one_get(tmp_path, monkeypatch):
    mirror = _Mirror(_dockets("EPA-2024-0001", "EPA-2025-0002"))
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: mirror)

    _run(tmp_path)

    assert sorted(mirror.gets) == sorted(mirror.objects), "retention must not add a second GET"
    events = {event["source_record_key"]: event for event in _retained_events(tmp_path)}
    assert set(events) == set(mirror.objects) == _manifest_keys(tmp_path)
    for key, content in mirror.objects.items():
        event = events[key]
        assert _archived(content) == content
        assert event["sha256"] == "sha256:" + hashlib.sha256(content).hexdigest()
        assert event["byte_size"] == len(content) and event["blob_store"] == "central-raw"
        assert event["source_url"] == f"s3://{mirrulations.BUCKET}/{key}"
        assert event["source_version"] == '"' + hashlib.md5(content, usedforsecurity=False).hexdigest() + '"'
        assert event["last_modified"] == MODIFIED.isoformat() and event["observed_at"]


def test_a_rerun_stores_no_duplicate_and_a_changed_object_is_a_new_version(tmp_path, monkeypatch):
    key = _docket_key("EPA-2024-0001")
    first = dumps(_docket_payload("EPA-2024-0001", "2024-01-01")).encode()
    changed = dumps(_docket_payload("EPA-2024-0001", "2024-09-09")).encode()
    for content in (first, first, changed):
        with MirrulationsOriginals(tmp_path, mirrulations.BUCKET, publish=False) as retention:
            retention.retain(key, content, {"ETag": "e"})
            retention.commit([key])

    objects = sorted(path.name for path in (_raw_root() / "objects/sha256").rglob("*") if path.is_file())
    assert objects == sorted({hashlib.sha256(first).hexdigest(), hashlib.sha256(changed).hexdigest()})
    assert _archived(first) == first and _archived(changed) == changed


def test_published_originals_reach_r2_before_the_merge_and_reruns_send_no_bytes(tmp_path, monkeypatch, r2_store):
    mirror = _Mirror(_dockets("EPA-2024-0001"))
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: mirror)
    monkeypatch.setattr(regulations.r2, "preflight_uploads", lambda *_: None)
    monkeypatch.setattr(regulations.r2, "upload_file", lambda *a, **k: None)
    (content,) = mirror.objects.values()
    blob = "source-evidence/blobs/sha256/" + hashlib.sha256(content).hexdigest()
    merged = regulations.merge_staging_files

    def merge(*args, **kwargs):
        assert r2_store.objects[blob] == content, "an original must be durable before its rows merge"
        return merged(*args, **kwargs)

    monkeypatch.setattr(regulations, "merge_staging_files", merge)
    _run(tmp_path, skip_upload=False)

    with MirrulationsOriginals(tmp_path, mirrulations.BUCKET, publish=True) as again:
        again.retain(next(iter(mirror.objects)), content, {})
        again.commit(mirror.objects)
    assert r2_store.sent[blob] == len(content), "a rerun's identical original is not sent again"


def test_a_retention_failure_refuses_before_any_merge_or_manifest(tmp_path, monkeypatch):
    mirror = _Mirror(_dockets("EPA-2024-0001", "EPA-2025-0002"))
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: mirror)
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    def broken(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(CentralRawArchiveStore, "put_blob", broken)
    monkeypatch.setattr(regulations, "merge_staging_files", lambda *a, **k: pytest.fail("merged unretained rows"))

    with pytest.raises(SourceEvidenceError, match="retention failed"):
        _run(tmp_path)
    assert _manifest_keys(tmp_path) == set()


def test_keys_the_resource_never_saw_are_not_retired(tmp_path, monkeypatch):
    """If SpicyDocs stopped reading through the supplied resource, the conservation check refuses."""
    mirror = _Mirror(_dockets("EPA-2024-0001"))
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: mirror)
    monkeypatch.setattr(originals_module.MirrulationsOriginals, "resource", lambda self, inner: inner)

    with pytest.raises(SourceEvidenceError, match="without a retained original"):
        _run(tmp_path)
    assert _manifest_keys(tmp_path) == set()


def test_an_incomplete_body_is_not_kept_as_an_original_and_stays_unresolved(tmp_path, monkeypatch):
    mirror = _Mirror(_dockets("EPA-2024-0001", "EPA-2025-0002"))
    short = _docket_key("EPA-2025-0002")
    mirror.declared[short] = len(mirror.objects[short]) + 1
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: mirror)

    _run(tmp_path)

    assert {event["source_record_key"] for event in _retained_events(tmp_path)} == {_docket_key("EPA-2024-0001")}
    assert _manifest_keys(tmp_path) == {_docket_key("EPA-2024-0001")}


def test_chunked_comments_retain_each_chunk_before_its_merge(tmp_path, monkeypatch):
    objects = {
        _comment_key(f"c{i}", "EPA-2026-0001"): dumps(
            _comment_payload(f"c{i}", "EPA-2026-0001", "2026-01-01T00:00:00Z")
        ).encode()
        for i in range(3)
    }
    mirror = _Mirror(objects)
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: mirror)
    merged: list[set[str]] = []
    monkeypatch.setattr(
        regulations.iceberg,
        "merge_comments",
        lambda *_: merged.append({e["source_record_key"] for e in _retained_events(tmp_path)}),
    )

    _run(tmp_path, skip_comments=False, only_comments=True, use_iceberg=True, enrich_text=False, chunk_size=2)

    assert [len(keys) for keys in merged] == [2, 3]
    assert {e["source_record_key"] for e in _retained_events(tmp_path)} == set(objects) == _manifest_keys(tmp_path)


def test_large_passes_split_into_bounded_evidence_artifacts(tmp_path, monkeypatch, r2_store):
    monkeypatch.setattr(originals_module, "OBJECTS_PER_ARTIFACT", 2)
    bodies = {f"raw-data/EPA/k{i}.json": f'{{"n": {i}}}'.encode() for i in range(5)}
    with MirrulationsOriginals(tmp_path, mirrulations.BUCKET, publish=True) as retention:
        for key, content in bodies.items():
            retention.retain(key, content, {})
        retention.commit(bodies)

    artifacts = {key.split("/")[1] for key in r2_store.objects if key.endswith("/artifact.json")}
    assert len(artifacts) == 3
    for content in bodies.values():
        assert r2_store.objects["source-evidence/blobs/sha256/" + hashlib.sha256(content).hexdigest()] == content


def test_the_scheduled_etl_keeps_its_originals_in_the_failure_artifact():
    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/etl-new-pipeline.yml").read_text())
    steps = workflow["jobs"]["etl-new"]["steps"]
    script = next(step["run"] for step in steps if step.get("name") == "Run new pipeline")
    assert 'export SPICYGOV_RAW_ROOT="$GITHUB_WORKSPACE/output/raw-source-originals"' in script
    assert next(step for step in steps if step.get("name") == "Upload artifacts (for debugging)")["with"]["path"] == "output/"
