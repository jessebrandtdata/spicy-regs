"""Tests for download_r2: error handling and atomic writes.

These guard against the March 2026 incident where a silent download
failure caused the ETL to overwrite a 3.3 GB historical comments.parquet
with a fresh empty file.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

import httpx
import pytest

from spicy_regs.sources import r2 as download_r2
from spicy_regs.sources.r2 import download_from_r2


@pytest.fixture(autouse=True)
def legacy_publication(monkeypatch):
    # This module exercises legacy transfer semantics. Managed members and
    # real index requests are exercised by test_generation_publication.
    from spicy_regs.sources import publication
    monkeypatch.setattr(publication, "load_index", lambda url: publication.empty_index())
    monkeypatch.setattr(publication.time, "sleep", lambda seconds: None)  # transport retries back off


def _fake_stream(*, status_code: int = 200, body: bytes = b"", raise_exc: Exception | None = None):
    """Build a callable that mimics ``httpx.stream(...)`` as a context manager."""

    @contextmanager
    def fake_stream(method, url, follow_redirects=True, **request):
        if raise_exc is not None:
            raise raise_exc
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_bytes = lambda: iter([body]) if body else iter([])
        yield resp

    return fake_stream


class TestDownloadFromR2:
    """Pins download_from_r2's contract: False only for an unset URL or a 404.

    Anything else — 5xx, a mid-stream drop, a connection error — raises and
    leaves no new or partial target and no ``.tmp`` staging file behind.
    """
    def test_returns_false_when_r2_url_not_set(self, tmp_path, monkeypatch):
        monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
        target = tmp_path / "foo.parquet"

        assert download_from_r2("foo.parquet", target) is False
        assert not target.exists()

    def test_returns_false_on_404(self, tmp_path, monkeypatch):
        """404 is a legitimate 'not on R2 yet' signal (first run)."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=404))

        target = tmp_path / "missing.parquet"
        assert download_from_r2("missing.parquet", target) is False
        assert not target.exists()

    def test_raises_on_server_error(self, tmp_path, monkeypatch):
        """5xx means the object may exist — we must not silently continue,
        or the pipeline will happily overwrite production data."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=503))

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="503"):
            download_from_r2("foo.parquet", target)
        assert not target.exists()

    def test_raises_on_connection_error(self, tmp_path, monkeypatch):
        """A connection error that outlasts the bounded retries raises, never "absent"."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(
            download_r2.httpx,
            "stream",
            _fake_stream(raise_exc=httpx.ConnectError("conn refused")),
        )

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="without progress") as raised:
            download_from_r2("foo.parquet", target)
        assert isinstance(raised.value.__cause__, httpx.ConnectError)
        assert not target.exists()

    def test_writes_file_on_success(self, tmp_path, monkeypatch):
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        monkeypatch.setattr(
            download_r2.httpx,
            "stream",
            _fake_stream(status_code=200, body=b"parquet-bytes"),
        )

        target = tmp_path / "foo.parquet"
        assert download_from_r2("foo.parquet", target) is True
        assert target.read_bytes() == b"parquet-bytes"

    def test_no_partial_file_on_mid_stream_error(self, tmp_path, monkeypatch):
        """If the stream raises mid-download, the destination file must not
        exist — otherwise callers will treat the corrupt partial as valid."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")

        @contextmanager
        def dying_stream(method, url, follow_redirects=True, **request):
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"first chunk"
                raise httpx.ReadError("connection dropped")

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", dying_stream)

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="no ETag to resume on") as raised:  # restarted to the cap
            download_from_r2("foo.parquet", target)
        assert isinstance(raised.value.__cause__, httpx.ReadError)
        assert not target.exists()

    def test_preserves_existing_file_on_failure(self, tmp_path, monkeypatch):
        """If target already exists and download fails, the old file must
        remain untouched."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")
        target = tmp_path / "foo.parquet"
        target.write_bytes(b"original content")

        monkeypatch.setattr(download_r2.httpx, "stream", _fake_stream(status_code=503))

        with pytest.raises(RuntimeError):
            download_from_r2("foo.parquet", target)

        assert target.read_bytes() == b"original content"

    def test_no_leftover_tmp_file_on_failure(self, tmp_path, monkeypatch):
        """The ``.tmp`` staging file must be cleaned up on any failure so
        it doesn't leak disk across retries."""
        monkeypatch.setenv("R2_PUBLIC_URL", "https://fake.r2.dev")

        @contextmanager
        def dying_stream(method, url, follow_redirects=True, **request):
            resp = MagicMock()
            resp.status_code = 200

            def iter_bytes():
                yield b"partial"
                raise httpx.ReadError("dropped")

            resp.iter_bytes = iter_bytes
            yield resp

        monkeypatch.setattr(download_r2.httpx, "stream", dying_stream)

        target = tmp_path / "foo.parquet"
        with pytest.raises(RuntimeError, match="no ETag to resume on"):
            download_from_r2("foo.parquet", target)

        # No lingering .tmp file either
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []
