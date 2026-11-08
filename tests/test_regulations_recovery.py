"""The host persists outcomes and never turns an empty/refused response into coverage."""

import json

import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from spicy_docs.schemas.regulations import RECORD_TYPES as SOURCE_RECORD_TYPES
from spicy_docs.sources import mirrulations

from spicy_regs.manifest import Manifest, save_manifest
from spicy_regs.pipelines.regulations import RegulationsPipeline
from spicy_regs.pipelines.regulations_state import UnresolvedKeys
from spicy_regs.schemas import DOCKET, RECORD_TYPES
from spicy_regs.sources import r2
from tests.test_regulations_pipeline import (
    _FakeS3Resource,
    _RaisingObj,
    _comment_key,
    _comment_payload,
    _docket_key,
    _docket_payload,
)


from spicy_regs.pipelines.regulatory_publication import restore_checkpoint
from tests.regulatory_publication_fakes import install as install_publication


@pytest.fixture(autouse=True)
def native_publication(monkeypatch):
    remote = install_publication(monkeypatch)
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    return remote


@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("body", [b"", b"{}", b"null", b"[]", b"42", b"{ broken"])
def test_unproductive_answer_is_retained_not_manifested(tmp_path, monkeypatch, chunked, body):
    key = _comment_key("c1", "EPA-2026-0001")
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: _FakeS3Resource({key: body}))
    pipeline = RegulationsPipeline(
        allow_fresh_start=True,
        agency="EPA",
        output_dir=tmp_path,
        only_comments=True,
        enrich_text=False,
        use_iceberg=True,
        chunk_size=1 if chunked else 0,
    )
    pipeline.run()
    pipeline.run()
    assert key not in Manifest.load(tmp_path, allow_fresh_start=True)
    rows = restore_checkpoint(tmp_path, "failed_keys")
    assert len(rows) == 1 and rows[0]["attempts"] == 2
    assert rows[0]["status"] == ("unreadable" if body == b"{ broken" else "requested-empty")
    assert not (tmp_path / "comments_index.parquet").exists()


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("chunked", [False, True])
def test_access_refusal_aborts_without_checkpoint(tmp_path, monkeypatch, status, chunked):
    key = _comment_key("c1", "EPA-2026-0001")

    class Refused(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            class Object:
                def get(self):
                    raise ClientError(
                        {"Error": {"Code": str(status)}, "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject"
                    )

            return Object()

    monkeypatch.setattr(mirrulations, "s3_resource", lambda: Refused({key: b"{}"}))
    with pytest.raises(mirrulations.MirrulationsAccessRefusedError):
        RegulationsPipeline(
            allow_fresh_start=True,
            agency="EPA",
            output_dir=tmp_path,
            only_comments=True,
            enrich_text=False,
            use_iceberg=True,
            chunk_size=1 if chunked else 0,
        ).run()
    assert not (tmp_path / "manifest.parquet").exists()
    assert not (tmp_path / "failed_keys.parquet").exists()


@pytest.mark.parametrize(
    "record_name,chunked",
    [
        *((name, False) for name in RECORD_TYPES),
        ("comments", True),
    ],
)
@pytest.mark.parametrize("transient_first", [False, True])
@pytest.mark.parametrize(
    "body",
    [
        {"data": {}},
        {"errors": [{"detail": "upstream failed"}]},
        {"data": {"id": "  "}},
        {"data": {"id": 42}},
        {"data": {"id": None}},
        {"data": {"id": ""}},
        {"id": "misplaced"},
        {"data": {"attributes": {"docketId": "EPA-2026-0001"}}},
    ],
)
def test_missing_record_identity_never_becomes_a_row_or_coverage(
    tmp_path,
    monkeypatch,
    record_name,
    chunked,
    body,
    transient_first,
):
    from spicy_regs.pipelines import regulations

    record_type = RECORD_TYPES[record_name]
    key = f"raw-data/EPA/EPA-2026-0001/text-record{record_type.path_pattern}record.json"

    class Resource(_FakeS3Resource):
        fail_next = transient_first

        def Object(self, name, key):  # noqa: N802
            if self.fail_next:
                self.fail_next = False
                return _RaisingObj()
            return super().Object(name, key)

    resource = Resource({key: json.dumps(body).encode()})
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: resource)
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda *a: pytest.fail("invalid record reached merge"))
    pipeline = RegulationsPipeline(
        allow_fresh_start=True,
        agency="EPA",
        output_dir=tmp_path,
        only_comments=record_name == "comments",
        skip_comments=record_name != "comments",
        enrich_text=False,
        use_iceberg=record_name == "comments",
        chunk_size=1 if chunked else 0,
    )
    pipeline.run()
    pipeline.run()
    assert key not in Manifest.load(tmp_path, allow_fresh_start=True)
    [row] = restore_checkpoint(tmp_path, "failed_keys")
    assert row["key"] == key and row["attempts"] == (3 if transient_first else 2)
    assert row["status"] == "requested-empty"
    assert f"missing nonblank data.id identity for {record_name} ({record_type.dedup_key})" in row["reason"]
    if "errors" in body:
        assert "publisher error" in row["reason"] and "upstream failed" in row["reason"]
    assert not list((tmp_path / "staging").rglob("*.parquet"))
    assert not (tmp_path / "dockets.parquet").exists()
    assert not (tmp_path / "documents.parquet").exists()
    assert not (tmp_path / "comments_index.parquet").exists()


@pytest.mark.parametrize("record_name", list(RECORD_TYPES))
def test_supplier_identity_matches_host_and_preserves_raw_fields(record_name):
    """Every ingested type keys on data.id; no parent docket ID is required."""
    host_type = RECORD_TYPES[record_name]
    source_type = SOURCE_RECORD_TYPES[record_name]
    assert source_type.dedup_key == host_type.dedup_key
    assert source_type.path_pattern == host_type.path_pattern
    body = {"data": {"id": "EPA-2026-0001", "attributes": {"title": "Unchanged"}}, "unknown": [1, 2]}
    reader = mirrulations.MirrulationsReader(
        _FakeS3Resource({"key": json.dumps(body).encode()}),
        "mirrulations",
        "raw-data",
        "EPA",
        source_type,
        key_lister=lambda: ["key"],
        download_workers=1,
    )
    assert list(reader.iter_records()) == [body]
    assert reader.last_keys == ["key"] and reader.unresolved == []
    expected = body["data"]["id"]
    assert host_type.extract(body)[host_type.dedup_key] == expected
    assert source_type.extract(body)[source_type.dedup_key] == expected


@pytest.mark.parametrize("record_name", list(RECORD_TYPES))
def test_supplier_rejects_publisher_error_even_with_identity(record_name):
    """The supplier is stricter than the removed guard and keeps scrubbed reasons."""
    secret = "synthetic-" + "publisher-secret"
    body = {"data": {"id": "EPA-2026-0001"}, "errors": [{"detail": "upstream failed api_key=" + secret}]}
    reader = mirrulations.MirrulationsReader(
        _FakeS3Resource({"key": json.dumps(body).encode()}),
        "mirrulations",
        "raw-data",
        "EPA",
        SOURCE_RECORD_TYPES[record_name],
        key_lister=lambda: ["key"],
        download_workers=1,
    )
    assert list(reader.iter_records()) == []
    assert reader.last_keys == [] and reader.failed_keys == ["key"]
    [outcome] = reader.unresolved
    assert outcome.status == "requested-empty" and outcome.attempts == 1
    assert "publisher error" in outcome.reason and "upstream failed" in outcome.reason
    assert secret not in outcome.reason


def test_selected_failed_coverage_is_retried_before_new_work(tmp_path, monkeypatch):
    old, new = _docket_key("EPA-2024-0001"), _docket_key("EPA-2025-0002")
    save_manifest(tmp_path, {old})
    from spicy_regs.pipelines.regulatory_publication import finish_checkpoints

    finish_checkpoints(tmp_path, {
        "failed_keys": [{"agency": "EPA", "record_type": "dockets", "key": old,
                         "status": "unreadable", "reason": "parse failed",
                         "attempted_at": "2026-09-19", "attempts": 1}],
        "pending_comment_text": [],
    }, publish=False)
    asked = []

    class Recording(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            asked.append(key)
            return super().Object(name, key)

    store = {
        key: json.dumps(_docket_payload(identifier, "2026-01-01")).encode()
        for key, identifier in ((new, "EPA-2025-0002"), (old, "EPA-2024-0001"))
    }
    resource = Recording(store)
    original = mirrulations.reader_factory
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: resource)
    monkeypatch.setattr(mirrulations, "reader_factory", lambda *a, **kw: original(*a, **kw, download_workers=1))
    RegulationsPipeline(allow_fresh_start=True, agency="EPA", output_dir=tmp_path, skip_comments=True).run()
    assert asked == [old, new]
    assert pq.read_table(tmp_path / "dockets.parquet").num_rows == 2
    assert len(restore_checkpoint(tmp_path, "failed_keys")) == 0


def test_scoped_run_preserves_other_agencies_outcomes(tmp_path):
    state = UnresolvedKeys(tmp_path)
    outcome = mirrulations.KeyOutcome("raw-data/FDA/key", "requested-empty", "empty-object", "2026-09-20", 3)
    state.update([], {("FDA", "dockets"): [outcome]})
    state.update([], {("EPA", "dockets"): []})
    assert UnresolvedKeys(tmp_path).for_reader("FDA", DOCKET) == [outcome]


@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("recovers", [False, True])
def test_unresolved_history_survives_fresh_hosted_runner(tmp_path, monkeypatch, chunked, recovers, native_publication):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    from spicy_regs import manifest as manifest_module
    from spicy_regs.pipelines import regulations

    remote = {}
    uploaded = []
    from spicy_regs.sources import publication

    native_publication.before_put = lambda key: (
        uploaded.append("checkpoints") if key == publication.INDEX_V2_KEY else None
    )
    asked = []
    old = _comment_key("old", "EPA-2026-0001") if chunked else _docket_key("EPA-2026-0001")
    new = _comment_key("new", "EPA-2026-0001") if chunked else _docket_key("EPA-2026-0002")
    store = {old: b'{"data":{}}'}

    def download(key, path):
        if key not in remote:
            return False
        path.write_bytes(remote[key])
        return True

    def upload(path, remote_key=None):
        key = remote_key or path.name
        # Exercise the real shrink decision, including a fully cleared checkpoint.
        r2._assert_upload_safe(path.stat().st_size, len(remote[key]) if key in remote else None, key)
        remote[key] = path.read_bytes()
        uploaded.append(key)

    class Recording(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            asked.append(key)
            return super().Object(name, key)

    original = mirrulations.reader_factory
    monkeypatch.setattr(mirrulations, "reader_factory", lambda *a, **kw: original(*a, **kw, download_workers=1))
    monkeypatch.setattr(mirrulations, "s3_resource", lambda: Recording(store))
    monkeypatch.setattr(r2, "download", download)
    monkeypatch.setattr(manifest_module, "download_from_r2", download)
    monkeypatch.setattr(r2, "upload_file", upload)
    monkeypatch.setattr(r2, "preflight_uploads", lambda *a: None)
    monkeypatch.setattr(r2, "upload_dataset", lambda out, names: [upload(out / f"{name}.parquet") for name in names])
    monkeypatch.setattr(regulations.iceberg, "merge_comments", lambda sd, rt: 1)

    def run(directory):
        RegulationsPipeline(
            allow_fresh_start=True,
            agency="EPA",
            output_dir=directory,
            only_comments=chunked,
            skip_comments=not chunked,
            enrich_text=False,
            use_iceberg=chunked,
            chunk_size=1 if chunked else 0,
            skip_upload=False,
        ).run()

    first, second = tmp_path / "first", tmp_path / "fresh-runner"
    run(first)
    assert uploaded == ["checkpoints"], "a zero-row pass must publish its unresolved state"
    [previous] = restore_checkpoint(first, "failed_keys")
    assert previous["attempts"] == 1
    uploaded.clear()
    asked.clear()
    valid_old = (
        _comment_payload("old", "EPA-2026-0001", "2026-01-01")
        if chunked
        else _docket_payload("EPA-2026-0001", "2026-01-01")
    )
    valid_new = (
        _comment_payload("new", "EPA-2026-0001", "2026-01-01")
        if chunked
        else _docket_payload("EPA-2026-0002", "2026-01-01")
    )
    # The old key is absent from discovery; only restored state can retry it.
    store = {new: json.dumps(valid_new).encode()}
    original_object = Recording.Object

    def object_with_old(self, name, key):
        if key == old:
            asked.append(key)
            return _FakeS3Resource({old: json.dumps(valid_old).encode() if recovers else b'{"data":{}}'}).Object(
                name, key
            )
        return original_object(self, name, key)

    monkeypatch.setattr(Recording, "Object", object_with_old)
    run(second)
    assert asked == [old, new], "restored unresolved work must precede new discovery"
    outcomes = UnresolvedKeys(second).rows
    if recovers:
        assert outcomes == {}
        assert old in Manifest.load(second)
    else:
        assert outcomes[old]["attempts"] == 2
        assert outcomes[old]["reason"] == previous["reason"]
        assert old not in Manifest.load(second)
    assert new in Manifest.load(second)
    assert uploaded[-2:] == ["checkpoints", "manifest.parquet"]
    third = tmp_path / "restore-only"
    third.mkdir()
    assert UnresolvedKeys(third, index=publication.current_index("https://test.invalid")).rows == outcomes, "cleared state must also survive a fresh runner"
