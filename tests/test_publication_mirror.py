"""Local publication mirror: the uploaded generation keys in the R2 layout, cloned at publish or fetched later."""
import httpx
import pytest
from rulespec_artifacts import LocalMemberSource

from spicy_regs.generations import admit_generation_for_publication
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_source_evidence import candidate, capture


def build(tmp_path):
    (tmp_path / "build").mkdir()
    evidence = CaptureEvidence(tmp_path / "build", "test")
    evidence.capture(capture(), stage="used")
    directory, _ = candidate(tmp_path / "build", evidence)
    return directory, evidence.artifact_dir


def publish(tmp_path, store, prior=None):
    directory, evidence = build(tmp_path)
    index = pub.publish_generation(directory, client=store, bucket="test", prior_index=prior or pub.empty_index(),
                                   evidence_directories=(evidence,))
    return directory, index, evidence


def tree(root):
    """The mirrored generation files, without the pointer snapshots."""
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*")
            if p.is_file() and p.relative_to(root).parts[0] != pub.SNAPSHOT_DIRECTORY}


def snapshots(root):
    return {p.name: p.read_bytes() for p in (root / pub.SNAPSHOT_DIRECTORY).iterdir()}


def uploaded(store, prefix):
    return {key[len(prefix) + 1:]: body for key, body in store.objects.items() if key.startswith(prefix + "/")}


def serve(monkeypatch, store):
    """Answer public GETs from the fake bucket; return the list of keys fetched."""
    fetched = []

    def stream(method, url, **kwargs):
        key = url.split("fixture.invalid/", 1)[1]
        fetched.append(key)
        body = store.objects.get(key)

        class Response:
            status_code = 404 if body is None else 200
            headers = httpx.Headers({})
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def iter_bytes(self): yield body or b""
        return Response()

    monkeypatch.setattr(pub.httpx, "stream", stream)
    return fetched


def test_mirror_is_off_unless_configured(tmp_path):
    _, index, _ = publish(tmp_path, Store())
    assert not (tmp_path / "mirror").exists()
    assert index.mirror is None


def test_publish_mirrors_exactly_the_uploaded_generation(tmp_path, monkeypatch):
    root = tmp_path / "mirror"
    monkeypatch.setenv(pub.PUBLICATION_MIRROR, str(root))
    store = Store()
    directory, index, _ = publish(tmp_path, store)
    prefix = index["families"]["test"]["prefix"]
    remote = uploaded(store, prefix)
    assert remote and tree(root) == {f"{prefix}/{key}": body for key, body in remote.items()}  # both directions
    assert set(pub.generation_files(index["families"]["test"])) == set(remote)  # the entry names the uploaded keys
    # A clone, not a hardlink: rewriting the build directory leaves the mirror unchanged.
    member = next(key for key in remote if key.endswith(".parquet"))
    assert (directory / member).stat().st_ino != (root / prefix / member).stat().st_ino
    (directory / member).write_bytes(b"edited later")
    assert (root / prefix / member).read_bytes() == remote[member]


def test_publish_snapshots_the_pointer_that_selected_the_generation(tmp_path, monkeypatch):
    """The owner's "selection manifest": the exact publication.v2.json bytes written, content-addressed."""
    import hashlib

    root = tmp_path / "mirror"
    monkeypatch.setenv(pub.PUBLICATION_MIRROR, str(root))
    store = Store()
    _, index, _ = publish(tmp_path, store)
    stored = store.objects[pub.INDEX_V2_KEY]
    name = f"{hashlib.sha256(stored).hexdigest()}.json"
    assert snapshots(root) == {name: stored}
    assert pub.parse_index(stored) == index
    assert not (root / pub.INDEX_V2_KEY).exists()  # the mutable pointer copy is never rewritten
    assert index.mirror == {"root": str(root), "prefix": index["families"]["test"]["prefix"], "kept": 0,
                            "cloned": len(pub.generation_files(index["families"]["test"])), "fetched": 0,
                            "missing": 0, "snapshot": f"{pub.SNAPSHOT_DIRECTORY}/{name}"}


def test_the_mirror_report_never_enters_a_serialized_index(tmp_path, monkeypatch):
    import copy
    import json

    monkeypatch.setenv(pub.PUBLICATION_MIRROR, str(tmp_path / "mirror"))
    _, index, _ = publish(tmp_path, Store())
    assert index.mirror is not None
    assert pub.parse_index(json.dumps(index).encode()) == index
    assert copy.deepcopy(index).mirror == index.mirror


def test_snapshot_refuses_bytes_that_are_not_an_index(tmp_path):
    with pytest.raises(pub.PublicationError):
        pub.mirror_index(tmp_path, b"{}")
    assert not (tmp_path / pub.SNAPSHOT_DIRECTORY).exists()


def test_republishing_the_same_generation_keeps_the_tree(tmp_path, monkeypatch):
    root = tmp_path / "mirror"
    monkeypatch.setenv(pub.PUBLICATION_MIRROR, str(root))
    store = Store()
    directory, index, evidence = publish(tmp_path, store)
    before = {p: p.stat().st_ino for p in root.rglob("*") if p.is_file()}
    again = pub.publish_generation(directory, client=store, bucket="test", prior_index=index,
                                   evidence_directories=(evidence,))
    assert again["families"]["test"] == index["families"]["test"]
    assert {p: p.stat().st_ino for p in root.rglob("*") if p.is_file()} == before  # kept, not rewritten


def test_mirror_failure_keeps_the_published_pointer(tmp_path, monkeypatch):
    root = tmp_path / "mirror"
    root.write_bytes(b"not a directory")
    monkeypatch.setenv(pub.PUBLICATION_MIRROR, str(root))
    store = Store()
    _, index, _ = publish(tmp_path, store)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == index
    assert index.mirror["prefix"] == index["families"]["test"]["prefix"]
    assert index.mirror["error"].startswith(("FileExistsError", "NotADirectoryError"))  # reported, not raised


def test_the_shared_seam_mirrors_for_every_publisher(tmp_path, monkeypatch):
    """Rollups, native conversion and remote promotion call _publish_verified_generation, not publish_generation."""
    root = tmp_path / "mirror"
    monkeypatch.setenv(pub.PUBLICATION_MIRROR, str(root))
    directory, evidence = build(tmp_path)
    store = Store()
    index = pub._publish_verified_generation(
        admit_generation_for_publication(directory), LocalMemberSource(directory), client=store, bucket="test",
        prior_index=pub.empty_index(), evidence_directories=(evidence,), local=directory,
        upload_member=lambda prefix, key: pub._put_immutable(store, "test", prefix + "/" + key, directory / key))
    prefix = index["families"]["test"]["prefix"]
    assert tree(root) == {f"{prefix}/{key}": body for key, body in uploaded(store, prefix).items()}


def test_catch_up_fetches_only_what_is_absent(tmp_path, monkeypatch):
    """The `spicy-regs mirror` path: no local build, public GETs checked against their pins."""
    store = Store()
    _, index, _ = publish(tmp_path, store)
    entry, root = index["families"]["test"], tmp_path / "mirror"
    fetched = serve(monkeypatch, store)
    first = pub.mirror_generation(root, entry, base_url="https://fixture.invalid")
    assert first == {"kept": 0, "cloned": 0, "fetched": len(pub.generation_files(entry)), "missing": 0}
    assert tree(root) == {f"{entry['prefix']}/{k}": v for k, v in uploaded(store, entry["prefix"]).items()}
    member = next(m for m in pub.generation_files(entry).values() if m.sha256 is not None)
    (root / member.path).unlink()
    fetched.clear()
    again = pub.mirror_generation(root, entry, base_url="https://fixture.invalid")
    assert fetched == [member.path] and again["fetched"] == 1 and again["kept"] == first["fetched"] - 1


def test_a_truncated_leftover_is_replaced_and_no_source_counts_missing(tmp_path, monkeypatch):
    store = Store()
    directory, index, _ = publish(tmp_path, store)
    entry, root = index["families"]["test"], tmp_path / "mirror"
    member = next(m for m in pub.generation_files(entry).values() if m.sha256 is not None)
    (root / member.path).parent.mkdir(parents=True)
    (root / member.path).write_bytes(b"short")
    counts = pub.mirror_generation(root, entry)  # neither a local build nor a public URL
    assert counts["missing"] == len(pub.generation_files(entry)) and counts["kept"] == 0
    counts = pub.mirror_generation(root, entry, local=directory)
    assert counts["cloned"] == len(pub.generation_files(entry))
    assert (root / member.path).read_bytes() == (directory / member.key).read_bytes()


def test_generation_files_include_receipts_and_their_key_index():
    pin = {"sha256": "sha256:" + "0" * 64, "byteSize": 1, "rows": 1}
    entry = {"prefix": "generations/f/" + "a" * 64, "tables": {"t.parquet": pin}, "etlReceipts": {
        "key": "etl_receipts.parquet", **pin, "keyIndex": {"key": "etl_receipts.keys.parquet", **pin}}}
    assert sorted(pub.generation_files(entry)) == ["artifact.json", "etl_receipts.keys.parquet",
                                                   "etl_receipts.parquet", "members.json", "t.parquet"]


def test_conftest_isolates_the_mirror_root():
    """An operator's SPICYGOV_PUBLICATION_MIRROR must never receive fixture generations."""
    import os

    assert pub.PUBLICATION_MIRROR not in os.environ


def run_mirror_command(tmp_path, monkeypatch, index, *arguments, free=2**50, reread=None):
    """Run `spicy-regs mirror` over ``index`` without the network; ``reread`` is the pointer seen before pruning."""
    import shutil
    import sys
    from types import SimpleNamespace

    from spicy_regs import cli

    calls, floors = [], []
    monkeypatch.setattr(shutil, "disk_usage", lambda path: floors.append(path) or SimpleNamespace(free=free))
    monkeypatch.setattr(pub, "load_index_bytes", lambda base: b"pointer")
    monkeypatch.setattr(pub, "parse_index", lambda raw: index)
    monkeypatch.setattr(pub, "load_index", lambda base: reread or index)
    monkeypatch.setattr(pub, "mirror_generation", lambda root, entry, base_url: calls.append(entry) or {"kept": 1})
    monkeypatch.setattr(pub, "mirror_index", lambda root, raw: root / pub.SNAPSHOT_DIRECTORY / "digest.json")
    monkeypatch.setattr(sys, "argv", ["spicy-regs", "mirror", str(tmp_path), *arguments])
    cli.main()
    return calls, floors


def lines(capsys):
    import json

    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def test_mirror_command_mirrors_each_named_family(tmp_path, monkeypatch, capsys):
    index = {"families": {"a": {"prefix": "generations/a/x", "tables": {}},
                          "b": {"prefix": "generations/b/y", "tables": {}}}}
    assert run_mirror_command(tmp_path, monkeypatch, index, "--family", "b")[0] == [index["families"]["b"]]
    assert lines(capsys) == [{"family": "b", "prefix": "generations/b/y", "kept": 1},
                             {"snapshot": f"{pub.SNAPSHOT_DIRECTORY}/digest.json"}]


def test_mirror_command_refuses_below_the_free_space_floor(tmp_path, monkeypatch):
    pin = {"sha256": "sha256:" + "0" * 64, "byteSize": 3 * 2**30, "rows": 1}
    index = {"families": {"a": {"prefix": "generations/a/" + "a" * 64, "tables": {"t.parquet": pin}}}}
    with pytest.raises(SystemExit, match="needs 3.0 GiB and 5.0 GiB is free"):
        run_mirror_command(tmp_path, monkeypatch, index, "--min-free-gib", "4", free=5 * 2**30)
    assert run_mirror_command(tmp_path, monkeypatch, index, "--min-free-gib", "1",
                              free=5 * 2**30)[0] == [index["families"]["a"]]


def test_nothing_to_fetch_is_never_refused_by_the_floor(tmp_path, monkeypatch, capsys):
    """`--prune` on a complete mirror deletes, so it must run on a volume already below the floor."""
    index = {"families": {"a": {"prefix": "generations/a/" + "a" * 64, "tables": {}}}}
    for name in ("a" * 64, "b" * 64):
        (tmp_path / "generations/a" / name).mkdir(parents=True)
    calls, floors = run_mirror_command(tmp_path, monkeypatch, index, "--prune", free=2**30)  # 1 GiB < 30
    assert calls == [index["families"]["a"]] and floors == []
    assert lines(capsys)[0]["pruned"] == ["b" * 64]
    assert sorted(p.name for p in (tmp_path / "generations/a").iterdir()) == ["a" * 64]


def test_prune_leaves_a_family_whose_selection_moved_during_the_run(tmp_path, monkeypatch, capsys):
    old, new = "a" * 64, "c" * 64
    index = {"families": {"a": {"prefix": f"generations/a/{old}", "tables": {}}}}
    moved = {"families": {"a": {"prefix": f"generations/a/{new}", "tables": {}}}}
    for name in (old, new):
        (tmp_path / "generations/a" / name).mkdir(parents=True)
    run_mirror_command(tmp_path, monkeypatch, index, "--prune", reread=moved)
    assert lines(capsys)[0] == {"family": "a", "prefix": f"generations/a/{old}", "kept": 1, "pruned": None,
                                     "selection_moved_to": f"generations/a/{new}"}
    assert sorted(p.name for p in (tmp_path / "generations/a").iterdir()) == [old, new]


def test_prune_keeps_only_the_selected_generation(tmp_path):
    selected, older = "a" * 64, "b" * 64
    entry = {"prefix": f"generations/f/{selected}", "tables": {}}
    for name in (selected, older, "notes"):
        (tmp_path / "generations/f" / name).mkdir(parents=True)
        (tmp_path / "generations/f" / name / "x").write_bytes(b"x")
    (tmp_path / "generations/g" / older).mkdir(parents=True)
    assert pub.prune_superseded(tmp_path, entry) == [tmp_path / "generations/f" / older]
    assert sorted(p.name for p in (tmp_path / "generations/f").iterdir()) == [selected, "notes"]
    assert (tmp_path / "generations/g" / older).is_dir()  # another family is untouched


def test_relative_mirror_root_is_resolved_to_an_absolute_path(tmp_path, monkeypatch):
    """ss-kvij: relative SPICYGOV_PUBLICATION_MIRROR paths are normalized to an absolute root in the receipt."""
    from pathlib import Path

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(pub.PUBLICATION_MIRROR, "./rel-mirror")
    _, published, _ = publish(tmp_path, Store())
    assert published.mirror is not None
    assert Path(published.mirror["root"]).is_absolute()
    assert Path(published.mirror["root"]) == (tmp_path / "rel-mirror").resolve()

