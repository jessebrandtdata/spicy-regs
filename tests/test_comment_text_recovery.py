"""Text failures survive raw checkpoints and fresh hosts; retries preserve other fields."""

import hashlib
import io
import json

import duckdb
import polars as pl
import pytest

from spicy_regs import manifest as manifest_module
from spicy_regs.pipelines import regulations
from spicy_regs.pipelines.comment_text import PENDING_TEXT_FILE, PendingCommentText
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2
from spicy_regs.sources.derived_text import DerivedTextUnavailable
from spicy_regs.transforms.derived_text_pool import DerivedTextPool, TextResult
from spicy_regs.sources.regulatory_catalog import processing_table
import pyarrow as pa


from tests.test_derived_text import ACF, _FakeS3Resource, _store, derived_key
from tests.test_regulations_pipeline import _comment_key, _comment_payload


from spicy_regs.pipelines.regulatory_publication import restore_checkpoint
from tests.regulatory_publication_fakes import install as install_publication


def _seed_catalog(con, rows):
    con.register("seed", pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in COMMENT.schema])))
    iceberg.replace_rows(con, COMMENT, "seed")


@pytest.fixture(autouse=True)
def native_publication(monkeypatch):
    remote = install_publication(monkeypatch)
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    return remote


@pytest.mark.parametrize("mode", ["catalog", "chunked"])
def test_text_retry_after_manifest_on_fresh_host(tmp_path, monkeypatch, mode, native_publication):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test.invalid")
    identity = f"{ACF[1]}-0004"
    raw_key = _comment_key(identity, ACF[1], agency=ACF[0])
    payload = _comment_payload(identity, ACF[1], "2025-01-01")
    payload["data"]["attributes"].update(agencyId=ACF[0], title="Preserved source title")
    payload["data"]["relationships"] = {"attachments": {"data": [{"id": "a", "type": "attachments"}]}}
    payload["included"] = [
        {
            "id": "a",
            "type": "attachments",
            "attributes": {
                "title": "Attachment",
                "fileFormats": [{"fileUrl": "https://example.org/a.pdf", "format": "pdf", "size": 10}],
            },
        }
    ]
    store = _store() | {raw_key: json.dumps(payload).encode()}
    failed = True
    gets = []

    class Resource(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            gets.append(key)
            obj = super().Object(name, key)
            if failed and key.startswith("derived-data/"):
                obj.e_tag = '"changed-after-listing"'
            return obj

    remote = {}
    uploads = []
    from spicy_regs.sources import publication

    native_publication.before_put = lambda key: (
        uploads.append("checkpoints") if key == publication.INDEX_V2_KEY else None
    )

    def download(key, path, **_):
        if key not in remote:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(remote[key])
        return True

    def upload(path, remote_key=None):
        key = remote_key or path.name
        uploads.append(key)
        remote[key] = path.read_bytes()

    monkeypatch.setattr(regulations.mirrulations, "s3_resource", lambda: Resource(store))
    monkeypatch.setattr(r2, "download_from_r2", download)
    monkeypatch.setattr(manifest_module, "download_from_r2", download)
    monkeypatch.setattr(r2, "preflight_uploads", lambda *args: None)
    monkeypatch.setattr(r2, "upload_file", upload)

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH '{tmp_path / 'catalog.duckdb'}' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, rt)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)

    def run(directory):
        regulations.RegulationsPipeline(
            output_dir=directory,
            agency=ACF[0],
            only_comments=True,
            allow_fresh_start=True,
            use_iceberg=True,
            chunk_size=1 if mode == "chunked" else 0,
            text_workers=2,
            skip_upload=False,
        ).run()

    def blob(content):
        return native_publication.objects.get("source-evidence/blobs/sha256/" + hashlib.sha256(content).hexdigest())

    first = tmp_path / "first"
    run(first)
    assert raw_key in set(pl.read_parquet(first / "manifest.parquet")["key"])
    assert blob(store[raw_key]) == store[raw_key], "the raw original reached R2"
    pending = restore_checkpoint(first, "pending_comment_text")
    assert pending[0]["comment_id"] == identity
    assert pending[0]["phase"] == "fetch" and pending[0]["attempts"] == 1
    assert json.loads(pending[0]["source_json"])["attachments"][0]["etag"]
    assert uploads[-1] == "manifest.parquet"
    assert uploads.index("checkpoints") < uploads.index("manifest.parquet")

    # The raw object is deliberately unavailable now: successful text recovery
    # must use the checkpoint and persisted row, never re-ingest the source JSON.
    del store[raw_key]
    failed = False
    gets.clear()
    text_key = derived_key(*ACF, "pypdf", "0004", 1)
    upsert = iceberg.upsert_comment_text

    def upsert_after_retention(*args, **kwargs):
        assert blob(store[text_key]) == store[text_key], "derived text is kept before its row is written"
        return upsert(*args, **kwargs)

    monkeypatch.setattr(iceberg, "upsert_comment_text", upsert_after_retention)
    second = tmp_path / "second"
    run(second)
    assert raw_key not in gets
    assert text_key in gets
    indexes = [io.BytesIO(raw) for key, raw in native_publication.objects.items()
               if key.startswith("source-evidence/indexes/regulations-mirrulations/")]
    indexed = pl.concat([pl.read_parquet(index) for index in indexes])
    assert {raw_key, text_key} <= set(indexed["key"])
    assert not restore_checkpoint(second, "pending_comment_text")
    with connect(COMMENT) as con:
        rows = con.execute(f"SELECT * FROM {processing_table(con, COMMENT)}").pl()
    row = rows.to_dicts()[0]
    assert row["text_content"] == "Wisconsin DCF comment body"
    assert row["text_extraction_status"] == "derived"
    assert row["title"] == "Preserved source title"
    assert row["modify_date"] == payload["data"]["attributes"]["modifyDate"]
    third = tmp_path / "third"
    third.mkdir()
    assert PendingCommentText(third).rows == {}


def test_retry_keeps_other_agencies_and_current_text_and_survives_failed_write(tmp_path, monkeypatch):
    monkeypatch.setattr(r2, "download", lambda *args: False)
    pending = PendingCommentText(tmp_path)
    rows = [
        {
            "agency_code": ACF[0],
            "docket_id": ACF[1],
            "comment_id": f"{ACF[1]}-{suffix}",
            "text_content": text,
            "text_extraction_status": status,
            "posted_date": "2025-01-01",
        }
        for suffix, text, status in [("0004", None, None), ("0015", "PDF result", "ok")]
    ]
    other = {**rows[0], "agency_code": "EPA", "docket_id": "EPA-2026-0001", "comment_id": "EPA-2026-0001-0001"}
    for row in [*rows, other]:
        pending.observe(TextResult(row, "failed", error=DerivedTextUnavailable("transport", phase="fetch")))
    pending.save()
    # A chunk may commit after a successful inline fetch whose metadata loses
    # deduplication. Its old retry must survive a crash before the final upsert.
    pending = PendingCommentText(tmp_path)
    pending.observe(TextResult(rows[0], "derived"))
    pending.save()
    assert rows[0]["comment_id"] in PendingCommentText(tmp_path).rows
    before = restore_checkpoint(tmp_path, "pending_comment_text")

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        _seed_catalog(con, rows)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)
    real_upsert = iceberg.upsert_comment_text

    def refuse(*args):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(iceberg, "upsert_comment_text", refuse)
    with DerivedTextPool(lambda: _FakeS3Resource(_store()), max_workers=2) as pool:
        with pytest.raises(RuntimeError, match="catalog unavailable"):
            PendingCommentText(tmp_path).retry(pool, tmp_path, [ACF[0]])
    assert restore_checkpoint(tmp_path, "pending_comment_text") == before

    verified = []

    def upsert(con, rt, agency, updates):
        real_upsert(con, rt, agency, updates)
        verified.extend(con.execute(f"SELECT comment_id, text_content FROM {iceberg._qualified(rt)}").fetchall())

    monkeypatch.setattr(iceberg, "upsert_comment_text", upsert)
    with DerivedTextPool(lambda: _FakeS3Resource(_store()), max_workers=2) as pool:
        resumed = PendingCommentText(tmp_path)
        # A raw row can lose the metadata merge by modify_date. Its successful
        # inline fetch must not retire a prior retry until persisted text exists.
        resumed.observe(TextResult(rows[0], "derived"))
        resumed.retry(pool, tmp_path, [ACF[0]])
        resumed.save()
    assert dict(verified)[rows[1]["comment_id"]] == "PDF result"
    assert dict(verified)[rows[0]["comment_id"]] == "Wisconsin DCF comment body"
    assert set(resumed.rows) == {other["comment_id"]}


def test_inline_access_refusal_never_advances_either_checkpoint(tmp_path, monkeypatch):
    from spicy_docs.sources.mirrulations import MirrulationsAccessRefusedError

    row = _comment_payload(f"{ACF[1]}-0004", ACF[1], "2025-01-01", agency=ACF[0])
    row["included"] = [
        {
            "id": "a",
            "type": "attachments",
            "attributes": {
                "fileFormats": [{"fileUrl": "https://example.org/a.pdf", "format": "pdf"}],
            },
        }
    ]
    key = _comment_key(row["data"]["id"], ACF[1], agency=ACF[0])
    raw = _FakeS3Resource({key: json.dumps(row).encode()})
    monkeypatch.setattr(r2, "download_from_r2", lambda *args, **kwargs: False)
    monkeypatch.setattr(manifest_module, "download_from_r2", lambda *args: False)
    # Pool resources are constructed before the agency reader's resource.
    resources = iter([_FakeS3Resource(_store(), listing_status=403), raw])
    monkeypatch.setattr(regulations.mirrulations, "s3_resource", lambda: next(resources))
    with pytest.raises(MirrulationsAccessRefusedError):
        regulations.RegulationsPipeline(
            output_dir=tmp_path,
            agency=ACF[0],
            only_comments=True,
            allow_fresh_start=True,
            text_workers=1,
            use_iceberg=True,
        ).run()
    assert not (tmp_path / "manifest.parquet").exists()
    assert not (tmp_path / PENDING_TEXT_FILE).exists()
    assert not (tmp_path / "comments").exists()


def test_null_docket_retry_matches_only_the_same_relationship(tmp_path, monkeypatch):
    monkeypatch.setattr(r2, "download", lambda *args: False)
    actual = [
        {
            **dict.fromkeys(COMMENT.schema),
            "comment_id": identity,
            "agency_code": "ODNI",
            "docket_id": docket,
            "text_content": "Already recovered",
            "text_extraction_status": "ok",
        }
        for identity, docket in [("unchanged", None), ("moved", "ODNI-known")]
    ]
    pending = PendingCommentText(tmp_path)
    for row in actual:
        pending.observe(
            TextResult({**row, "docket_id": None}, "failed", error=DerivedTextUnavailable("retry", phase="fetch"))
        )
    pending.save()

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        _seed_catalog(con, actual)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)
    monkeypatch.setattr(iceberg, "upsert_comment_text", lambda *a: pytest.fail("a filled row was overwritten"))
    with DerivedTextPool(lambda: _FakeS3Resource({}), max_workers=1) as pool:
        resumed = PendingCommentText(tmp_path)
        resumed.retry(pool, tmp_path, ["ODNI"])
        resumed.save()
    assert set(resumed.rows) == {"moved"}
