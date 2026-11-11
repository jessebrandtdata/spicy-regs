"""The pinned reader wheel is the one its receipt records, and its raw archive keeps exact originals."""

import gzip
import hashlib
import json
import re
import tomllib
import zipfile
from pathlib import Path

import pytest
from spicy_docs.storage.publication import ImmutablePublicationError
from spicy_docs.storage.raw_archive import CentralRawArchiveStore


def test_pinned_reader_wheel_matches_its_receipt():
    """`uv sync --frozen` already holds the wheel to the lock's hash; this holds the receipt to the wheel."""
    root = Path(__file__).parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text())
    wheel = root / project["tool"]["uv"]["sources"]["spicy-docs"]["path"]
    version = wheel.name.removeprefix("spicy_docs-").removesuffix("-py3-none-any.whl")
    # A numbered release only: a local build such as 0.86.0+foo.1 is not pinned without changing this line.
    assert re.fullmatch(r"\d+\.\d+\.\d+", version), version
    receipt = json.loads((root / f"vendor/spicy_docs-{version}.json").read_text())
    assert receipt["filename"] == wheel.name and receipt["version"] == version
    assert receipt["byte_size"] == wheel.stat().st_size
    assert hashlib.sha256(wheel.read_bytes()).hexdigest() == receipt["sha256"]
    with zipfile.ZipFile(wheel) as archive:
        assert {"spicy_docs/storage/raw_archive.py", "spicy_docs/reading/refusals.py"} <= set(archive.namelist())


@pytest.mark.parametrize("existing", [None, "raw", "gzip"])
def test_stream_capture_reuses_exact_original_without_duplicate_wrapper(tmp_path, existing):
    body = b"publisher bytes\r\n\x00" * 20
    digest = hashlib.sha256(body).hexdigest()
    store = CentralRawArchiveStore(tmp_path)
    original = tmp_path / "objects/sha256" / digest[:2] / digest
    if existing is not None:
        original = original.with_suffix(".gz") if existing == "gzip" else original
        original.parent.mkdir(parents=True)
        original.write_bytes(gzip.compress(body, mtime=0) if existing == "gzip" else body)
    result = store.put_stream(iter([body[:20], body[20:]]), max_bytes=len(body))
    assert result.blob_ref == "sha256:" + digest
    assert result.byte_size == len(body) and result.reused == (existing is not None)
    assert [path for path in (tmp_path / "objects").rglob("*") if path.is_file()] == [original]
    with store.open(result.blob_ref) as stream:
        assert stream.read() == body


@pytest.mark.parametrize("failure", ["overflow", "interrupted"])
def test_stream_failure_publishes_no_partial_original(tmp_path, failure):
    store = CentralRawArchiveStore(tmp_path)

    def chunks():
        yield b"first"
        if failure == "interrupted":
            raise OSError("source interrupted")
        yield b"beyond limit"

    error = OSError if failure == "interrupted" else ImmutablePublicationError
    with pytest.raises(error):
        store.put_stream(chunks(), max_bytes=5)
    assert not [path for path in tmp_path.rglob("*") if path.is_file()]
