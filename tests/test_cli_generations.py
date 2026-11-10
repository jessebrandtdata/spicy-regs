"""Real local batch selection with hermetic remote bytes and failure injection.

Pins ``cli.cmd_download``'s managed-batch semantics: one content-named snapshot
directory per batch, switched by replacing ``current`` only after every member
verifies, with member, pointer and user-path failures leaving the prior batch,
its bytes and its incomplete-run evidence intact.
"""

import argparse
import builtins
import hashlib
import io
import json
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

import httpx
import polars as pl
import pytest

from spicy_regs import cli
from spicy_regs.sources import publication


def _remote(monkeypatch, label="old", *, legacy=False):
    bodies = {}
    tables = {}
    for name in ("a", "b"):
        buffer = io.BytesIO()
        pl.DataFrame({"id": [f"{label}-{name}"], "title": [f"{label} title"]}).write_parquet(buffer)
        body = buffer.getvalue()
        bodies[name] = body
        tables[f"{name}.parquet"] = {
            "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "byteSize": len(body),
            "rows": 1,
            "columns": [["id", "VARCHAR"], ["title", "VARCHAR"]],
        }
    index = publication.empty_index()
    digest = hashlib.sha256(label.encode()).hexdigest()
    index["families"]["pair"] = {
        "prefix": f"generations/pair/{digest}",
        "logicalId": "urn:test:pair",
        "artifactDigest": "sha256:" + digest,
        "tables": tables,
    }
    index = publication.parse_index(json.dumps(index).encode())
    loads, requests = [], []

    def load(_):
        loads.append(True)
        return deepcopy(index)

    @contextmanager
    def stream(method, url, **kwargs):
        requests.append(url)
        name = url.rsplit("/", 1)[-1].removesuffix(".parquet")
        if name in {"a", "b"}:
            assert url == f"{cli.resolve_r2_base_url()}/{index['families']['pair']['prefix']}/{name}.parquet"
        yield httpx.Response(200, content=bodies[name], request=httpx.Request(method, url))

    if legacy:
        bodies["dockets"] = bodies["a"]
    monkeypatch.setattr(publication, "load_index", load)
    monkeypatch.setattr(cli.httpx, "stream", stream)
    return index, bodies, loads, requests


def _download(output, *names):
    cli.cmd_download(argparse.Namespace(output_dir=output, types=list(names), force=False))
    return (output / "current").resolve(strict=True)


def test_complete_managed_batch_records_one_snapshot_and_switches_together(tmp_path, monkeypatch):
    index, _, loads, requests = _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    assert len(loads) == 1 and len(requests) == 2
    metadata = json.loads((old / "download.json").read_text())
    assert metadata["publication"] == index
    assert metadata["status"] == "complete"
    assert set(metadata["selected"]) == {"a", "b"}
    assert not (tmp_path / "a.parquet").exists()
    _remote(monkeypatch, "new")
    new = _download(tmp_path, "a", "b")
    assert new != old
    assert pl.read_parquet(old / "a.parquet")["id"].to_list() == ["old-a"]
    assert pl.read_parquet(new / "a.parquet")["id"].to_list() == ["new-a"]
    assert pl.read_parquet(new / "b.parquet")["id"].to_list() == ["new-b"]


def test_managed_batch_uses_configured_host_for_snapshot_metadata_and_members(tmp_path, monkeypatch):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fork.example/")
    _, _, loads, requests = _remote(monkeypatch)
    current = _download(tmp_path, "a", "b")
    assert len(loads) == 1
    assert len(requests) == 2 and all(url.startswith("https://fork.example/generations/") for url in requests)
    assert json.loads((current / "download.json").read_text())["base_url"] == "https://fork.example"


def test_host_switch_to_legacy_replaces_current_without_losing_previous_bytes(tmp_path, monkeypatch):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    monkeypatch.setattr(publication, "load_index", lambda base: publication.empty_index())
    requests = []

    @contextmanager
    def stream(method, url, **kwargs):
        requests.append(url)
        buffer = io.BytesIO()
        pl.DataFrame({"docket_id": [url]}).write_parquet(buffer)
        yield httpx.Response(200, content=buffer.getvalue(), request=httpx.Request(method, url))

    monkeypatch.setattr(cli.httpx, "stream", stream)
    for host in ("https://fork.example", cli.PUBLIC_URL):
        monkeypatch.setenv("R2_PUBLIC_URL", host)
        current = _download(tmp_path, "dockets")
        path, status = cli._local_selection(tmp_path).files["dockets"]
        assert path == current / "dockets.parquet"
        assert status == "legacy-unversioned"
        assert pl.read_parquet(path)["docket_id"].to_list() == [f"{host}/dockets.parquet"]
        metadata = json.loads((current / "download.json").read_text())
        assert metadata["base_url"] == host
        assert metadata["selected"]["dockets"]["status"] == "legacy-unversioned"
        assert current != old
        assert pl.read_parquet(old / "a.parquet")["id"].to_list() == ["old-a"]
    assert requests == ["https://fork.example/dockets.parquet", f"{cli.PUBLIC_URL}/dockets.parquet"]


def test_failed_legacy_host_switch_keeps_prior_current(tmp_path, monkeypatch):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    monkeypatch.setenv("R2_PUBLIC_URL", "https://fork.example")
    monkeypatch.setattr(publication, "load_index", lambda base: publication.empty_index())

    @contextmanager
    def unavailable(method, url, **kwargs):
        assert url == "https://fork.example/dockets.parquet"
        yield httpx.Response(503, request=httpx.Request(method, url))

    monkeypatch.setattr(cli.httpx, "stream", unavailable)
    with pytest.raises(RuntimeError, match="Download incomplete"):
        _download(tmp_path, "dockets")
    assert (tmp_path / "current").resolve() == old
    assert pl.read_parquet(cli._local_selection(tmp_path).files["a"][0])["id"].to_list() == ["old-a"]


@pytest.mark.parametrize("failure", ["hash", "http"])
def test_member_failure_retains_prior_current_and_incomplete_evidence(tmp_path, monkeypatch, failure):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    _, bodies, _, _ = _remote(monkeypatch, "new")
    if failure == "hash":
        bodies["b"] = b"corrupted"
    else:
        real_stream = cli.httpx.stream

        @contextmanager
        def fail(method, url, **kwargs):
            if url.endswith("/b.parquet"):
                raise httpx.ConnectError("interrupted")
            with real_stream(method, url, **kwargs) as response:
                yield response

        monkeypatch.setattr(cli.httpx, "stream", fail)
        from spicy_regs.sources import publication

        monkeypatch.setattr(publication.time, "sleep", lambda seconds: None)  # the member fetch retries, then refuses
    with pytest.raises(RuntimeError, match="Download incomplete: b"):
        _download(tmp_path, "a", "b")
    assert (tmp_path / "current").resolve() == old
    failed = next(path for path in (tmp_path / "download-runs").iterdir() if path != old)
    assert (failed / "a.parquet").exists()
    assert not (failed / "b.parquet").exists()
    assert json.loads((failed / "download.json").read_text())["status"] == "incomplete"


def test_pointer_failure_preserves_old_current(tmp_path, monkeypatch):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    _remote(monkeypatch, "new")
    replace = Path.replace

    def fail_current(self, target):
        if Path(target).name == "current":
            raise OSError("cannot switch current")
        return replace(self, target)

    monkeypatch.setattr(Path, "replace", fail_current)
    with pytest.raises(OSError, match="cannot switch"):
        _download(tmp_path, "a", "b")
    assert (tmp_path / "current").resolve() == old
    assert not list(tmp_path.glob(".current-*"))


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_current_does_not_replace_existing_user_path(tmp_path, monkeypatch, kind):
    _remote(monkeypatch)
    current = tmp_path / "current"
    if kind == "directory":
        current.mkdir()
        saved = current / "user-file"
    else:
        saved = current
    saved.write_text("preserve me")
    with pytest.raises(RuntimeError, match="preserving existing path"):
        _download(tmp_path, "a", "b")
    assert saved.read_text() == "preserve me"
    assert not current.is_symlink()


@pytest.mark.parametrize("damage", ["missing-member", "incomplete-metadata"])
def test_invalid_current_does_not_fall_back_to_stale_root(tmp_path, monkeypatch, damage):
    _remote(monkeypatch)
    batch = _download(tmp_path, "a", "b")
    (tmp_path / "a.parquet").write_bytes((batch / "a.parquet").read_bytes())
    if damage == "missing-member":
        (batch / "a.parquet").unlink()
    else:
        metadata = json.loads((batch / "download.json").read_text())
        metadata["status"] = "incomplete"
        (batch / "download.json").write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError, match="Current download"):
        cli._local_selection(tmp_path)


def test_mixed_batch_labels_legacy_and_readers_prefer_selected_files(tmp_path, monkeypatch, capsys):
    _remote(monkeypatch, legacy=True)
    # A selected download excludes every unselected root file.
    pl.DataFrame({"id": ["stale-root"], "title": ["stale"]}).write_parquet(tmp_path / "a.parquet")
    pl.DataFrame({"id": ["standalone"], "title": ["legacy"]}).write_parquet(tmp_path / "other.parquet")
    batch = _download(tmp_path, "a", "b", "dockets")
    metadata = json.loads((batch / "download.json").read_text())
    assert metadata["selected"]["dockets"] == {"key": "dockets.parquet", "status": "legacy-unversioned"}
    args = argparse.Namespace(output_dir=tmp_path, data_type="a", agency=None, n=1, query="old", limit=5)
    cli.cmd_stats(args)
    cli.cmd_sample(args)
    cli.cmd_search(args)
    output = capsys.readouterr().out
    assert "legacy-unversioned" in output and "managed" in output
    assert "old-a" in output and "old-b" in output and "stale-root" not in output
    assert "OTHER" not in output


def test_reader_holds_one_local_batch_if_current_switches_during_operation(tmp_path, monkeypatch, capsys):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    _remote(monkeypatch, "new")
    new = _download(tmp_path, "a", "b")
    (tmp_path / "current").unlink()
    (tmp_path / "current").symlink_to(old)
    read = pl.read_parquet
    visited = []

    def switch_after_first(paths, **kwargs):
        visited.extend(Path(path) for path in paths)
        if len(visited) == 1:
            (tmp_path / "current").unlink()
            (tmp_path / "current").symlink_to(new)
        return read(paths, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", switch_after_first)
    cli.cmd_search(argparse.Namespace(output_dir=tmp_path, query="old", limit=5))
    assert visited == [old / "a.parquet", old / "b.parquet"]
    assert "old-b" in capsys.readouterr().out


def test_legacy_only_download_keeps_root_behavior(tmp_path, monkeypatch, capsys):
    _, bodies, _, _ = _remote(monkeypatch, legacy=True)
    cli.cmd_download(argparse.Namespace(output_dir=tmp_path, types=["dockets"], force=False))
    assert (tmp_path / "dockets.parquet").read_bytes() == bodies["dockets"]
    assert not (tmp_path / "current").exists()
    assert "legacy-unversioned" in capsys.readouterr().out


@pytest.mark.parametrize("name", ["../other", "a/b", "a.parquet", "A", "", "a?b"])
def test_table_names_refuse_paths(name):
    with pytest.raises(argparse.ArgumentTypeError):
        cli._table_name(name)


@pytest.mark.parametrize("command", ["download", "sample"])
def test_parser_accepts_rollup_names(monkeypatch, command):
    seen = []
    monkeypatch.setattr(cli, f"cmd_{command}", lambda args: seen.append(args))
    monkeypatch.setattr(
        "sys.argv", ["spicy-regs", command, *(["--types"] if command == "download" else []), "bill_actions"]
    )
    cli.main()
    assert (seen[0].types if command == "download" else [seen[0].data_type]) == ["bill_actions"]


def test_managed_download_does_not_import_optional_source_readers_or_mcp(tmp_path, monkeypatch):
    _remote(monkeypatch)
    original = builtins.__import__

    def base_install_only(name, *args, **kwargs):
        if name.split(".")[0] in {"spicy_docs", "rulespec_artifacts"} or name == "spicy_regs.mcp_server":
            raise ImportError(f"Optional module unavailable: {name}")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", base_install_only)
    _download(tmp_path, "a", "b")


def test_receipts_download_once_per_selected_family_and_are_guarded(tmp_path, monkeypatch):
    from spicy_regs.local_data import local_selection, verify_local_members, receipt_local_key
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.etl_receipts import RECEIPT_SCHEMA
    from spicy_regs.native_types import described_schema
    index, bodies, _, requests = _remote(monkeypatch)
    output = io.BytesIO()
    pq.write_table(pa.Table.from_batches([], schema=RECEIPT_SCHEMA), output)
    body = output.getvalue()
    bodies["etl_receipts"] = body
    index["families"]["pair"]["etlReceipts"] = {
        "key": "etl_receipts.parquet", "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
        "byteSize": len(body), "rows": 0, "columns": [list(c) for c in described_schema(RECEIPT_SCHEMA)],
        "generationId": "capture-a", "datasets": ["a", "b"]}
    current = _download(tmp_path, "a", "b")
    selection = local_selection(tmp_path)
    assert len(selection.receipts) == 1
    member, = publication.receipt_members(index, dataset="a")
    receipt = current / receipt_local_key(member)
    assert receipt.read_bytes() == body
    assert len([url for url in requests if url.endswith("etl_receipts.parquet")]) == 1
    assert str(receipt) in verify_local_members(selection)
    receipt.write_bytes(b"different")
    with pytest.raises(RuntimeError, match="receipt member differs"):
        verify_local_members(selection)


def test_failed_receipt_download_keeps_previous_current(tmp_path, monkeypatch):
    index, bodies, _, _ = _remote(monkeypatch)
    old = _download(tmp_path, "a")
    index["families"]["pair"]["etlReceipts"] = {
        "key": "etl_receipts.parquet", "sha256": "sha256:" + "a" * 64,
        "byteSize": 1, "rows": 0, "columns": [["receipt_id", "VARCHAR"]],
        "generationId": "capture-a", "datasets": ["a", "b"]}
    bodies["etl_receipts"] = b"wrong bytes"
    with pytest.raises(RuntimeError, match="differs from its pin"):
        _download(tmp_path, "a")
    assert (tmp_path / "current").resolve() == old


def test_two_family_receipt_names_never_collide(tmp_path, monkeypatch):
    from spicy_regs.local_data import local_selection, verify_local_members
    index, bodies, _, _ = _remote(monkeypatch)
    first = index["families"]["pair"]
    other = deepcopy(first)
    other.update(prefix="generations/second/" + "b" * 64, artifactDigest="sha256:" + "b" * 64,
                 tables={"b.parquet": first["tables"].pop("b.parquet")})
    index["families"]["second"] = other
    receipt_bodies = {"pair": b"receipt-a", "second": b"receipt-b"}
    for family, entry in index["families"].items():
        body = receipt_bodies[family]
        entry["etlReceipts"] = {"key": "etl_receipts.parquet", "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "byteSize": len(body), "rows": 0, "columns": [["receipt_id", "VARCHAR"]],
            "generationId": family, "datasets": ["a" if family == "pair" else "b"]}
    @contextmanager
    def stream(method, url, **kwargs):
        family = "second" if "/second/" in url else "pair"
        name = url.rsplit("/", 1)[-1].removesuffix(".parquet")
        body = receipt_bodies[family] if name == "etl_receipts" else bodies[name]
        yield httpx.Response(200, content=body, request=httpx.Request(method, url))
    monkeypatch.setattr(cli.httpx, "stream", stream)
    _download(tmp_path, "a", "b")
    selection = local_selection(tmp_path)
    assert len(selection.receipts) == 2
    assert {path.read_bytes() for path in selection.receipts.values()} == set(receipt_bodies.values())
    assert len(verify_local_members(selection)) == 4
