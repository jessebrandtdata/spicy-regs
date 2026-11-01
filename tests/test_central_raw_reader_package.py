"""The central archive adapter preserves current readers and interrupted writes."""

import gzip
import hashlib
import json
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest
from spicy_docs.storage.publication import ImmutablePublicationError
from spicy_docs.storage.raw_archive import CentralRawArchiveStore


def test_overlay_reproduces_and_preserves_current_reader_modules():
    root = Path(__file__).parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text())
    wheel = root / project["tool"]["uv"]["sources"]["spicy-docs"]["path"]
    if "+centralraw." in wheel.name:
        receipt = json.loads(wheel.with_suffix(".json").read_text())
        assert hashlib.sha256(wheel.read_bytes()).hexdigest() == receipt["sha256"]
        changed = set(receipt["changed_package_files"])
        assert changed == {"spicy_docs/storage/raw_archive.py", "spicy_docs/reading/refusals.py"}
        with zipfile.ZipFile(root / "vendor" / receipt["base_wheel"]) as base, zipfile.ZipFile(wheel) as overlay:
            original_modules = {name for name in base.namelist()
                                if name.startswith("spicy_docs/") and not name.endswith("/")}
            assert {name for name in overlay.namelist() if name.startswith("spicy_docs/")} == original_modules | changed
            for name in original_modules - changed:
                assert overlay.read(name) == base.read(name), name
    else:
        version = wheel.name.removeprefix("spicy_docs-").removesuffix("-py3-none-any.whl")
        receipt = json.loads((root / f"vendor/spicy_docs-{version}.json").read_text())
        assert receipt["filename"] == wheel.name and receipt["version"] == version
        assert hashlib.sha256(wheel.read_bytes()).hexdigest() == receipt["sha256"]
        with zipfile.ZipFile(wheel) as archive:
            names = set(archive.namelist())
            assert {"spicy_docs/storage/raw_archive.py", "spicy_docs/reading/refusals.py"} <= names
    subprocess.run([sys.executable, str(root / "scripts/package_central_raw_reader.py"), "--check"],
                   cwd=root, check=True, capture_output=True)


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
