"""Retained producer bytes remain immutable when current main publishes them."""

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from spicy_regs import generations
from spicy_regs import native_conversion as conversion
from tests.test_native_conversion import (
    BUCKET, CFR, MAIN, STATE, WHEEL,
    bucket as _bucket, cfr_on_its_receipt_route as _cfr_on_its_receipt_route,
    PREPARED, convert, prepared_digest, publish_old, stored,
)

bucket = _bucket
#: cfr-sections on its former receipt route, this file's example family (autouse; tests/test_native_conversion.py).
cfr_on_its_receipt_route = _cfr_on_its_receipt_route
PUBLISHER = "be" * 20


def _git(root, *arguments):
    return subprocess.run(["git", "-C", str(root), *arguments], check=True, capture_output=True,
                          text=True).stdout.strip()


@pytest.fixture
def history(tmp_path):
    root = tmp_path / "history"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.email", "test@example.invalid")
    _git(root, "config", "user.name", "Test")
    package = root / "src/spicy_regs"
    package.mkdir(parents=True)
    # Path component sorting differs from simple string sorting for a.py versus a/nested.py.
    (package / "a").mkdir()
    (package / "a/nested.py").write_bytes(b"retained = 'producer'\n")
    (package / "a.py").write_bytes(b"value = 1\n")
    (package / "__pycache__").mkdir()
    (package / "__pycache__/ignore.py").write_bytes(b"not source\n")
    (root / "vendor").mkdir()
    wheel = b"retained locked wheel bytes"
    (root / "vendor/spicy_docs.whl").write_bytes(wheel)
    lock = {'package': [{'name': 'spicy-docs', 'version': WHEEL,
                         'source': {'path': 'vendor/spicy_docs.whl'},
                         'wheels': [{'hash': 'sha256:' + hashlib.sha256(wheel).hexdigest()}]}]}
    (root / "uv.lock").write_text(
        f'[[package]]\nname = "spicy-docs"\nversion = "{WHEEL}"\n'
        'source = {path = "vendor/spicy_docs.whl"}\n'
        f'wheels = [{{hash = "{lock["package"][0]["wheels"][0]["hash"]}"}}]\n'
    )
    _git(root, "add", ".")
    _git(root, "commit", "-m", "Retained producer")
    producer = _git(root, "rev-parse", "HEAD")
    identity = "urn:spicy-regs:implementation:sha256:" + generations.source_digest(package)
    (package / "a.py").write_bytes(b"value = 2\n")
    _git(root, "commit", "-am", "Current publisher")
    publisher = _git(root, "rev-parse", "HEAD")
    return root, producer, publisher, identity, lock["package"][0]


def test_historical_identity_uses_exact_git_blobs_and_generation_path_order(history):
    root, producer, publisher, identity, locked = history
    # Neither working files nor archive export transformations may change historical source hashing.
    (root / "src/spicy_regs/a.py").write_bytes(b"different working bytes\n")
    assert conversion._historical_producer(root, producer, publisher) == (identity, locked)


@pytest.mark.parametrize("damage", ["short", "unavailable", "not-ancestor", "locked-runtime", "wheel"])
def test_historical_pin_runtime_and_wheel_refuse(history, damage):
    root, producer, publisher, _, _ = history
    if damage == "short":
        producer = producer[:12]
    elif damage == "unavailable":
        producer = "aa" * 20
    elif damage == "not-ancestor":
        producer, publisher = publisher, producer
    elif damage == "locked-runtime":
        with (root / "uv.lock").open("a") as stream:
            stream.write('\n# changed locked runtime\n')
        _git(root, "commit", "-am", "Changed runtime")
        publisher = _git(root, "rev-parse", "HEAD")
    else:
        (root / "vendor/spicy_docs.whl").write_bytes(b"wrong historical wheel")
        _git(root, "commit", "-am", "Wrong wheel bytes")
        producer = publisher = _git(root, "rev-parse", "HEAD")
    with pytest.raises(conversion.ConversionRefused):
        conversion._historical_producer(root, producer, publisher)


def _historical_fixture(monkeypatch, identity=None):
    calls = []

    def historical(root, producer, publisher, **_):
        calls.append((producer, publisher))
        assert producer == MAIN and publisher == PUBLISHER
        return identity or generations.implementation_id(), {
            "version": WHEEL, "wheels": [{"hash": STATE["spicy_docs_wheel"]["locked_sha256"]}],
        }

    monkeypatch.setattr(conversion, "_historical_producer", historical)
    return calls


def _publish(path, **overrides):
    arguments: dict = dict(allowed=["cfr-sections"], expected_main=PUBLISHER, expected_spicy_docs=WHEEL,
                     expect_bucket=BUCKET, expected_producer=MAIN, expected_receipt_digest=prepared_digest(path),
                     state=lambda _: STATE | {"checkout": PUBLISHER, "main": PUBLISHER}) | overrides
    return conversion.publish_prepared(path, **arguments)


def test_distinct_publisher_preserves_producer_seal_generation_and_rollback(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    dry = convert("cfr-sections", work)
    generation = Path(dry["generation"]["directory"])
    before = {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()}
    calls = _historical_fixture(monkeypatch)

    def no_writer(*args, **kwargs):
        raise AssertionError("Prepared publication must not run a writer")

    monkeypatch.setattr(conversion, "_convert_rollup", no_writer)
    done = _publish(work / conversion.RECEIPT)
    assert calls == [(MAIN, PUBLISHER)]
    assert done["source"] == dry["source"]
    assert done["prepared"] == dry["prepared"]
    assert done["generation"] == dry["generation"]
    assert {p.relative_to(generation): p.read_bytes() for p in generation.rglob("*") if p.is_file()} == before
    publisher = done["publish_attempt"]["publisher"]
    assert publisher == {"source": STATE | {"checkout": PUBLISHER, "main": PUBLISHER},
                         "implementationId": generations.implementation_id()}
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert stored(bucket) == old


@pytest.mark.parametrize("damage", ["missing-pin", "wrong-pin", "identity", "history", "runtime", "dirty", "moved"])
def test_distinct_producer_refusals_do_not_write_or_modify_the_sealed_receipt(tmp_path, monkeypatch, bucket, damage):
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    _historical_fixture(monkeypatch, identity="wrong" if damage == "identity" else None)
    if damage == "runtime":
        # A seal without recorded loaded code keeps the identical-runtime rule (resumed producers, older seals).
        receipt = json.loads(path.read_text())
        del receipt["prepared"]["closure"]
        receipt["prepared"]["receiptDigest"] = conversion._prepared_digest(receipt)
        path.write_text(json.dumps(receipt))
        PREPARED[path.resolve()] = receipt["prepared"]["receiptDigest"]
    overrides = {}
    if damage in {"missing-pin", "wrong-pin"}:
        overrides["expected_producer"] = None if damage == "missing-pin" else "af" * 20
    elif damage == "history":
        def unavailable(*args, **_):
            raise conversion.ConversionRefused("Unavailable retained history")
        monkeypatch.setattr(conversion, "_historical_producer", unavailable)
    elif damage in {"runtime", "dirty"}:
        state = STATE | {"checkout": PUBLISHER, "main": PUBLISHER}
        if damage == "runtime":
            state["spicy_docs_wheel"] = STATE["spicy_docs_wheel"] | {
                "locked_sha256": "sha256:" + "ca" * 32, "file_sha256": "sha256:" + "ca" * 32,
            }
        else:
            state["uncommitted"] = [" M src/changed.py"]
        overrides["state"] = lambda _: state
    elif damage == "moved":
        states = iter([STATE | {"checkout": PUBLISHER, "main": PUBLISHER}, STATE])
        overrides["state"] = lambda _: next(states)
    before = list(bucket.writes), path.read_bytes()
    with pytest.raises(conversion.ConversionRefused):
        _publish(path, **overrides)
    assert (bucket.writes, path.read_bytes()) == before
    assert stored(bucket) == old
    assert "publish_attempt" not in json.loads(path.read_bytes())


def test_the_seal_records_the_code_it_loaded(tmp_path, monkeypatch, bucket):
    """The seal holds the bytes of the loaded mapper, the package data files and loaded distributions (ss-5tt8)."""
    import hashlib

    from spicy_regs import code_closure

    publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    closure = json.loads((work / conversion.RECEIPT).read_text())["prepared"]["closure"]
    mapper = "spicy_regs/pipelines/rollups/cfr_sections.py"
    # Derived independently of code_closure: hash the file the import system names for the mapper module.
    import spicy_regs.pipelines.rollups.cfr_sections as module
    expected = "sha256:" + hashlib.sha256(open(module.__file__, "rb").read()).hexdigest()
    assert closure["modules"][mapper] == expected
    assert "spicy_regs/native_conversion.py" in closure["modules"]
    assert closure["data"] and all(not key.endswith(".py") for key in closure["data"])
    assert "duckdb" in closure["distributions"] and closure["python"]
    # sys.modules is process-wide; which builders a fresh conversion loads is checked in a subprocess below.
    assert set(closure) == {"modules", "data", "distributions", "python", "platform"}
    assert code_closure.record()["modules"][mapper] == expected
def _moved_runtime():
    """A publisher on a later main whose lock and SpicyDocs wheel moved: the old rule refused this outright."""
    state = STATE | {"checkout": PUBLISHER, "main": PUBLISHER}
    state["spicy_docs_wheel"] = STATE["spicy_docs_wheel"] | {
        "locked_sha256": "sha256:" + "ca" * 32, "file_sha256": "sha256:" + "ca" * 32}
    return lambda _: dict(state)


def _installed_copy(tmp_path, monkeypatch, package):
    """This process's ``package`` as a separate tree the closure reads, so one file can change without touching it."""
    import shutil

    from spicy_regs import code_closure

    real = code_closure._root
    copy = tmp_path / "installed" / package
    shutil.copytree(real(package), copy, ignore=shutil.ignore_patterns("__pycache__"))
    monkeypatch.setattr(code_closure, "_root", lambda name: copy if name == package else real(name))
    return copy


def _sealed_closure(path):
    return json.loads(path.read_text())["prepared"]["closure"]


@pytest.mark.parametrize("change", [None, "spicy-regs-outside", "spicy-docs-outside"])
def test_a_moved_runtime_rebinds_on_byte_identical_loaded_code(tmp_path, monkeypatch, bucket, change):
    """A lock or wheel move, or an edit to code the preparation never loaded, publishes from the later main."""
    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    closure = _sealed_closure(path)
    if change is not None:
        package = "spicy_regs" if change == "spicy-regs-outside" else "spicy_docs"
        copy = _installed_copy(tmp_path, monkeypatch, package)
        outside = next(name for name in sorted(copy.rglob("*.py"))
                       if f"{package}/{name.relative_to(copy).as_posix()}" not in closure["modules"])
        outside.write_text(outside.read_text() + "\n# an unrelated edit\n")
    calls = []
    real = conversion._historical_producer

    def historical(root, producer, publisher, **options):
        calls.append(options)
        return generations.implementation_id(), {"version": WHEEL, "wheels": [{"hash": STATE["spicy_docs_wheel"]["locked_sha256"]}]}

    monkeypatch.setattr(conversion, "_historical_producer", historical)
    assert real is not historical
    done = _publish(path, state=_moved_runtime())
    assert calls == [{"same_lock": False}]
    assert done["publish_attempt"]["verification"]["rebound_by_loaded_code"] is True
    assert done["publish_attempt"]["verification"]["mode"] == "sealed-admission"
    conversion.rollback(path, expect_bucket=BUCKET)
    assert stored(bucket) == old


@pytest.mark.parametrize("change", ["lossless-mapper", "data-file", "distribution"])
def test_a_change_to_loaded_code_refuses_the_rebind(tmp_path, monkeypatch, bucket, change):
    """Even a change that restores every row the same (a comment) refuses: the rule is bytes, not outcomes."""
    from importlib import metadata

    old = publish_old(bucket, monkeypatch, tmp_path, "cfr-sections", {"cfr_sections": CFR})
    work = tmp_path / "work"
    convert("cfr-sections", work)
    path = work / conversion.RECEIPT
    closure = _sealed_closure(path)
    if change == "distribution":
        held = next(iter(closure["distributions"]))
        real_version = metadata.version
        from spicy_regs import code_closure
        monkeypatch.setattr(code_closure, "version", lambda name: "0.0.0" if name == held else real_version(name))
    else:
        copy = _installed_copy(tmp_path, monkeypatch, "spicy_regs")
        if change == "lossless-mapper":
            mapper = "spicy_regs/pipelines/rollups/cfr_sections.py"
            assert mapper in closure["modules"]
            target = copy / mapper.removeprefix("spicy_regs/")
            target.write_text(target.read_text() + "\n# a comment-only mapper change\n")
        else:
            target = next(copy.rglob("*.json"))
            target.write_bytes(target.read_bytes() + b" ")
    _historical_fixture(monkeypatch)
    before = list(bucket.writes), path.read_bytes()
    with pytest.raises(conversion.ConversionRefused, match="loaded code differs"):
        _publish(path, state=_moved_runtime())
    assert (bucket.writes, path.read_bytes()) == before and stored(bucket) == old


def test_the_conversion_path_imports_no_unrelated_builders_or_spicy_docs():
    """Loading one transform no longer loads every builder; the publish entry point loads no SpicyDocs."""
    import subprocess
    import sys

    code = ("import sys, spicy_regs.native_conversion; "
            "assert not any(n.startswith('spicy_docs') for n in sys.modules), 'native_conversion'; "
            "import spicy_regs.transforms.regulations_receipts; "
            "assert not any(n.startswith('spicy_regs.transforms.build_') for n in sys.modules), 'builders'")
    subprocess.run([sys.executable, "-c", code], check=True)


def test_importing_source_evidence_loads_no_spicy_docs_modules():
    """Importing spicy_regs.source_evidence in a fresh process does not eagerly load spicy_docs modules (ss-5tt8)."""
    import sys

    code = (
        "import sys, spicy_regs.source_evidence; "
        "loaded = sorted(n for n in sys.modules if n == 'spicy_docs' or n.startswith('spicy_docs.')); "
        "assert not loaded, f'unexpected spicy_docs modules loaded: {loaded}'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_package_tree_and_code_closure_agree_on_package_data_files(tmp_path, monkeypatch):
    """generations._package_tree and code_closure._data_files share one package data-file boundary (ss-5tt8)."""
    from spicy_regs import code_closure

    copy = _installed_copy(tmp_path, monkeypatch, "spicy_regs")
    monkeypatch.setattr(generations, "__file__", str(copy / "generations.py"))

    before_tree = generations._package_tree(copy)
    before_identity = generations.validation_identity()["spicy_regs"]
    before_data = code_closure._data_files()
    assert before_tree == before_identity

    # Ignored bytecode / cache files do not affect either authority.
    pycache = copy / "__pycache__"
    pycache.mkdir(exist_ok=True)
    (pycache / "cached.sql").write_text("SELECT 1;\n")
    (copy / "orphan.pyc").write_bytes(b"bytecode")
    assert generations._package_tree(copy) == before_tree
    assert code_closure._data_files() == before_data

    # Non-.py/.pyc package data files (.sql, .txt) move both validation_identity() and code_closure._data_files().
    sql_file = copy / "schema.sql"
    sql_file.write_text("SELECT 1;\n")
    after_sql_tree = generations._package_tree(copy)
    after_sql_identity = generations.validation_identity()["spicy_regs"]
    after_sql_data = code_closure._data_files()
    assert after_sql_tree == after_sql_identity != before_tree
    assert "spicy_regs/schema.sql" in after_sql_data

    sql_file.write_text("SELECT 2;\n")
    assert generations.validation_identity()["spicy_regs"] != after_sql_identity
    assert code_closure._data_files()["spicy_regs/schema.sql"] != after_sql_data["spicy_regs/schema.sql"]

