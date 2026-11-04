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


def _private_consumer(tmp_path, *, version="0.68.0+rawguard.1"):
    """Copy only the helper's pinned inputs; tests never edit the checkout's vendor bytes."""
    import shutil

    root = Path(__file__).parents[1]
    consumer = tmp_path / "consumer"
    (consumer / "scripts").mkdir(parents=True)
    (consumer / "vendor").mkdir()
    shutil.copy2(root / "scripts/package_central_raw_reader.py", consumer / "scripts")
    project = tomllib.loads((root / "pyproject.toml").read_text())
    current = Path(project["tool"]["uv"]["sources"]["spicy-docs"]["path"]).name.removeprefix(
        "spicy_docs-"
    ).removesuffix("-py3-none-any.whl")
    current_receipt = json.loads((root / f"vendor/spicy_docs-{current}.json").read_text())
    receipt = json.loads((root / f"vendor/spicy_docs-{version}.json").read_text())
    for name in [receipt["filename"], f"spicy_docs-{version}.json"]:
        shutil.copy2(root / "vendor" / name, consumer / "vendor" / name)
    for name in ["pyproject.toml", "uv.lock"]:
        text = (root / name).read_text().replace(current, version)
        text = text.replace(current_receipt["sha256"], receipt["sha256"])
        (consumer / name).write_text(text)
    return consumer, receipt


def _run_package_check(consumer, *options):
    return subprocess.run([sys.executable, str(consumer / "scripts/package_central_raw_reader.py"), *options],
                          cwd=consumer, capture_output=True, text=True)


@pytest.mark.parametrize("options", [[], ["--check"], ["--write"]])
def test_qualified_private_native_check_preserves_all_input_bytes(tmp_path, options):
    consumer, receipt = _private_consumer(tmp_path)
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns)
              for path in consumer.rglob("*") if path.is_file()}
    result = _run_package_check(consumer, *options)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["qualification"] == "private_native"
    assert report["source_commit"] == receipt["source_commit"]
    assert report["sha256"] == receipt["sha256"]
    assert {path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in consumer.rglob("*") if path.is_file()} == before


def test_numbered_native_release_still_uses_retained_release_validation(tmp_path):
    consumer, receipt = _private_consumer(tmp_path, version="0.68.0")
    result = _run_package_check(consumer, "--check")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["qualification"] == "release"
    assert json.loads(result.stdout)["sha256"] == receipt["sha256"]


@pytest.mark.parametrize("damage, message", [
    ("wheel", "does not match its qualified digest"),
    ("lock", "does not match its recorded digest"),
    ("source", "not the reviewed identity"),
    ("receipt-size", "source receipt identities disagree"),
    ("unknown-qualifier", "current reader pin is not"),
])
def test_unqualified_private_pin_or_changed_inputs_refuse_without_writes(tmp_path, damage, message):
    consumer, receipt = _private_consumer(tmp_path)
    wheel = consumer / "vendor" / receipt["filename"]
    receipt_path = consumer / "vendor/spicy_docs-0.68.0+rawguard.1.json"
    if damage == "wheel":
        raw = wheel.read_bytes()
        wheel.write_bytes(raw[:100] + bytes([raw[100] ^ 1]) + raw[101:])
    elif damage == "lock":
        lock = consumer / "uv.lock"
        lock.write_text(lock.read_text().replace(receipt["sha256"], "a" * 64))
    elif damage in {"source", "receipt-size"}:
        receipt["source_commit" if damage == "source" else "byte_size"] = (
            "a" * 40 if damage == "source" else receipt["byte_size"] + 1
        )
        receipt_path.write_text(json.dumps(receipt))
    else:
        for name in ["pyproject.toml", "uv.lock"]:
            path = consumer / name
            path.write_text(path.read_text().replace("+rawguard.1", "+rawguard.2"))
    before = {path: path.read_bytes() for path in consumer.rglob("*") if path.is_file()}
    result = _run_package_check(consumer, "--write")
    assert result.returncode != 0 and message in result.stderr
    assert {path: path.read_bytes() for path in consumer.rglob("*") if path.is_file()} == before
