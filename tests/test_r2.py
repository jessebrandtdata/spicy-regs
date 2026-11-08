"""Pins the R2 storage connector's shrink guards and publication behavior.

Preflight runs before the first write, a partition upload refuses a missing
index before writing, purge targets the exact URL, and per-table failures are
all reported.
"""

from pathlib import Path

import pytest

import spicy_regs.sources.r2 as r2


def test_retry_checkpoint_can_clear_without_disabling_dataset_shrink_guards():
    r2._assert_upload_safe(100, 100_000, "failed_keys.parquet")
    r2._assert_upload_safe(100, 100_000, "pending_comment_text.parquet")
    for key in ("manifest.parquet", "dockets.parquet", "comments_index.parquet"):
        with pytest.raises(RuntimeError, match="shrink"):
            r2._assert_upload_safe(100, 100_000, key)


def test_download_delegates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []
    monkeypatch.setattr(r2, "download_from_r2", lambda key, path: calls.append((key, path)) or True)

    assert r2.download("manifest.parquet", tmp_path / "manifest.parquet") is True
    assert calls == [("manifest.parquet", tmp_path / "manifest.parquet")]


def test_upload_dataset_uploads_only_existing_base_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The dataset publisher leaves the manifest to its pipeline caller."""
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "documents.parquet").write_bytes(b"x")
    (tmp_path / "manifest.parquet").write_bytes(b"m")
    # comments.parquet intentionally absent — it's published as partitions.

    uploaded: list[Path] = []
    monkeypatch.setattr(r2, "preflight_uploads", lambda output_dir, files: None)
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: uploaded.append(p))

    r2.upload_dataset(tmp_path, ["dockets", "documents", "comments"])

    assert set(uploaded) == {
        tmp_path / "dockets.parquet",
        tmp_path / "documents.parquet",
    }


def test_upload_comment_partitions_uploads_changed_and_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """upload_comment_partitions publishes each changed partition and the index."""
    changed = tmp_path / "comments" / "agency_code=EPA" / "part-0.parquet"
    changed.parent.mkdir(parents=True)
    changed.write_bytes(b"c")
    (tmp_path / "comments_index.parquet").write_bytes(b"i")

    uploaded: list[tuple[Path, str | None]] = []
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: uploaded.append((p, remote_key)))

    r2.upload_comment_partitions(tmp_path, [changed])

    assert (changed, str(changed.relative_to(tmp_path))) in uploaded
    assert (tmp_path / "comments_index.parquet", "comments_index.parquet") in uploaded


def test_upload_comment_partitions_refuses_without_an_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing index refuses before any partition is written, not after."""
    changed = tmp_path / "comments" / "agency_code=EPA" / "part-0.parquet"
    changed.parent.mkdir(parents=True)
    changed.write_bytes(b"c")

    uploaded: list[Path] = []
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: uploaded.append(p))

    with pytest.raises(RuntimeError, match="comments_index.parquet"):
        r2.upload_comment_partitions(tmp_path, [changed])
    assert uploaded == []


def _upload_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal R2 env so upload_file runs its body (isolate_env strips these)."""
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "s")
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.spicy-regs.dev")


def test_upload_file_purges_exact_object_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """After a successful upload, the edge cache is purged for that one URL."""
    _upload_env(monkeypatch)
    (tmp_path / "agency_stats.parquet").write_bytes(b"x" * 100)

    monkeypatch.setattr(r2, "get_r2_client", lambda: __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock())
    monkeypatch.setattr(r2, "_get_remote_size", lambda *a, **k: None)
    purged: list[list[str]] = []
    monkeypatch.setattr(r2, "purge_urls", lambda urls: purged.append(urls))

    r2.upload_file(tmp_path / "agency_stats.parquet")

    assert purged == [["https://data.spicy-regs.dev/agency_stats.parquet"]]


def test_upload_file_survives_unreachable_edge(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: a dead Cloudflare edge must not fail a successful publish.

    Uses the real ``purge_urls`` (creds set) with ``httpx.post`` raising, so this
    exercises the actual composition rather than a mock — the property that
    matters is that the ETL never dies because the cache couldn't be purged.
    """
    _upload_env(monkeypatch)
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "tok")
    monkeypatch.setenv("CLOUDFLARE_ZONE_ID", "zone")
    (tmp_path / "agency_stats.parquet").write_bytes(b"x" * 100)

    monkeypatch.setattr(r2, "get_r2_client", lambda: __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock())
    monkeypatch.setattr(r2, "_get_remote_size", lambda *a, **k: None)

    import httpx

    def _raise(*a, **k):
        raise httpx.ConnectError("edge down")

    monkeypatch.setattr(httpx, "post", _raise)

    # Must complete normally despite the purge failing underneath.
    r2.upload_file(tmp_path / "agency_stats.parquet")


@pytest.mark.parametrize("failures", [r2.UPLOAD_ATTEMPTS - 1, r2.UPLOAD_ATTEMPTS])
def test_upload_file_repeats_a_failed_transfer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failures: int) -> None:
    """R2 can answer a multipart complete with InvalidPart; the whole transfer repeats, bounded."""
    from boto3.exceptions import S3UploadFailedError

    _upload_env(monkeypatch)
    (tmp_path / "comments.parquet").write_bytes(b"x" * 100)
    calls: list[str] = []

    class Client:
        def upload_file(self, path: str, bucket: str, key: str, ExtraArgs: dict) -> None:  # noqa: N803 - boto3's name
            calls.append(key)
            if len(calls) <= failures:
                raise S3UploadFailedError("An error occurred (InvalidPart) when calling the CompleteMultipartUpload operation")

    monkeypatch.setattr(r2, "get_r2_client", Client)
    monkeypatch.setattr(r2, "_get_remote_size", lambda *a, **k: None)
    monkeypatch.setattr(r2, "purge_urls", lambda urls: None)
    if failures < r2.UPLOAD_ATTEMPTS:
        r2.upload_file(tmp_path / "comments.parquet")
    else:
        with pytest.raises(S3UploadFailedError):
            r2.upload_file(tmp_path / "comments.parquet")
    assert calls == ["comments.parquet"] * min(failures + 1, r2.UPLOAD_ATTEMPTS)


def test_upload_dataset_raises_when_an_upload_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failing publish must surface rather than die inside the executor.

    Regression: `executor.map`'s lazy iterator was discarded, so the shrink
    guard's refusal to overwrite dockets.parquet never propagated and the ETL
    reported success while the table sat frozen for 8 weeks.
    """
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "documents.parquet").write_bytes(b"x")

    def fake_upload(path: Path, remote_key: str | None = None) -> None:
        if path.name == "dockets.parquet":
            raise RuntimeError("Refusing to upload dockets.parquet: would shrink remote")

    monkeypatch.setattr(r2, "preflight_uploads", lambda output_dir, files: None)
    monkeypatch.setattr(r2, "upload_file", fake_upload)

    with pytest.raises(RuntimeError, match="dockets.parquet"):
        r2.upload_dataset(tmp_path, ["dockets", "documents"])


def test_upload_dataset_preflights_all_guards_before_uploading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A known shrink refusal must stop the whole dataset before the first write."""
    _upload_env(monkeypatch)
    (tmp_path / "documents.parquet").write_bytes(b"x" * 100)
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    # The manifest is the pipeline's to publish last; upload_dataset ignores it.
    (tmp_path / "manifest.parquet").write_bytes(b"m" * 100)

    monkeypatch.setattr(r2, "get_r2_client", lambda: object())
    checked: list[str] = []

    def fake_remote_size(client: object, bucket: str, key: str) -> int:
        checked.append(key)
        return 100

    monkeypatch.setattr(
        r2,
        "_get_remote_size",
        fake_remote_size,
    )
    uploaded: list[Path] = []
    monkeypatch.setattr(r2, "upload_file", lambda path, remote_key=None: uploaded.append(path))

    with pytest.raises(RuntimeError, match="Publication preflight failed.*dockets.parquet"):
        r2.upload_dataset(tmp_path, ["documents", "dockets"])

    assert uploaded == []
    assert checked == ["documents.parquet", "dockets.parquet"]


def test_upload_dataset_reports_every_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One broken table must not hide another behind it."""
    (tmp_path / "dockets.parquet").write_bytes(b"d")
    (tmp_path / "documents.parquet").write_bytes(b"x")

    def fake_upload(path: Path, remote_key: str | None = None) -> None:
        raise RuntimeError(f"boom: {path.name}")

    monkeypatch.setattr(r2, "preflight_uploads", lambda output_dir, files: None)
    monkeypatch.setattr(r2, "upload_file", fake_upload)

    with pytest.raises(RuntimeError) as excinfo:
        r2.upload_dataset(tmp_path, ["dockets", "documents"])

    message = str(excinfo.value)
    assert "dockets.parquet" in message
    assert "documents.parquet" in message
    assert "2 of 2" in message


def test_upload_dataset_is_a_noop_with_nothing_to_publish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An empty publish set must not blow up on max_workers=0."""
    monkeypatch.setattr(r2, "upload_file", lambda p, remote_key=None: None)

    r2.upload_dataset(tmp_path, ["dockets"])


@pytest.mark.parametrize("fault", [None, "bytes", "version", "missing-etag"])
def test_public_readback_binds_bytes_to_stable_storage_version(tmp_path, monkeypatch, fault):
    from contextlib import contextmanager
    import hashlib
    import httpx

    data = b"complete candidate bytes"
    path = tmp_path / "comments.parquet"
    path.write_bytes(data)
    version = {"etag": '"stable"', "bytes": len(data)}
    versions = iter([version, {**version, "etag": '"changed"'} if fault == "version" else version])
    monkeypatch.setattr(r2, "object_version", lambda _: next(versions))

    @contextmanager
    def stream(method, url, **kwargs):
        assert kwargs["headers"] == {"User-Agent": "spicy-regs", "Accept-Encoding": "identity"}
        headers = {} if fault == "missing-etag" else {"etag": '"stable"'}
        yield httpx.Response(200, headers=headers, content=b"x" * len(data) if fault == "bytes" else data,
                             request=httpx.Request(method, url))

    monkeypatch.setattr(r2.httpx, "stream", stream)
    if fault:
        with pytest.raises(RuntimeError, match="readback differs"):
            r2.verify_public_file(path, "comments.parquet", "https://public.example")
    else:
        assert r2.verify_public_file(path, "comments.parquet", "https://public.example") == {
            **version, "sha256": hashlib.sha256(data).hexdigest(),
        }


def test_public_version_asks_for_the_stored_bytes(monkeypatch):
    """The custom domain answers a JSON object gzip-encoded, with a weak ETag and no length, unless asked not to."""
    import httpx

    def head(url, **options):
        assert options["headers"]["User-Agent"] == "spicy-regs"
        request = httpx.Request("HEAD", url)
        if url.endswith("/absent.json"):
            return httpx.Response(404, request=request)
        if options["headers"].get("Accept-Encoding") != "identity":
            return httpx.Response(200, headers={"etag": 'W/"fbc6"', "content-encoding": "gzip"}, request=request)
        return httpx.Response(200, headers={"etag": '"fbc6"', "content-length": "228490"}, request=request)

    monkeypatch.setattr(r2.httpx, "head", head)
    assert r2.public_object_version("https://public.example/publication.v2.json") == {"etag": '"fbc6"', "bytes": 228490}
    assert r2.public_object_version("https://public.example/absent.json") is None


@pytest.mark.parametrize("status", [403, 500])
def test_public_version_preserves_provider_refusals(monkeypatch, status):
    import httpx

    def head(url, **options):
        assert options["headers"] == {"User-Agent": "spicy-regs", "Accept-Encoding": "identity", "Cache-Control": "no-cache"}
        return httpx.Response(status, request=httpx.Request("HEAD", url))

    monkeypatch.setattr(r2.httpx, "head", head)
    with pytest.raises(httpx.HTTPStatusError):
        r2.public_object_version("https://public.example/publication.v2.json")


@pytest.mark.parametrize("code", ["NoSuchKey", "AccessDenied", "InternalError"])
def test_receipt_and_object_version_only_treat_absence_as_optional(monkeypatch, code):
    from botocore.exceptions import ClientError

    class Client:
        def get_object(self, **kwargs):
            raise ClientError({"Error": {"Code": code}}, "GetObject")

        head_object = get_object

    monkeypatch.setattr(r2, "get_r2_client", Client)
    for read in (r2.read_json_object, r2.object_version):
        if code == "NoSuchKey":
            assert read("comments-publication.json") is None
        else:
            with pytest.raises(ClientError):
                read("comments-publication.json")


def test_a_publish_without_credentials_fails_instead_of_skipping(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A caller that reaches the uploader meant to publish; its dry run is its own ``skip_upload``.

    The fork's GAO rollup ran green every day from 2026-09-05 to 09-21 with empty
    secrets, each run logging "Skipping upload" and publishing nothing.
    """
    for name in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: pytest.fail("no client without credentials"))
    table = tmp_path / "gao_reports.parquet"
    table.write_bytes(b"x")

    with pytest.raises(RuntimeError, match="R2 credentials"):
        r2.upload_file(table)
    with pytest.raises(RuntimeError, match="R2 credentials"):
        r2.preflight_uploads(tmp_path, [table])
