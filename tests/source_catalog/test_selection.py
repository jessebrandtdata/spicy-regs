"""Local catalog selection validates candidates and restores interrupted switches."""

import json
from pathlib import Path

import pytest

from spicy_regs.source_catalog.exporter import export
from spicy_regs.source_catalog import selection
from spicy_regs.source_catalog.storage import connect, digest, schema


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def prepared(tmp_path):
    raw, cache, public, staged = [tmp_path / name for name in ("raw", "cache", "public", "staged")]
    cache.mkdir()
    write(raw / "snapshot.json", {"snapshot": "retained"})
    write(raw / "acquisitions/sha256/ab/receipt.json", {"receipt": "retained"})
    old, new = "sha256:" + "a" * 64, "sha256:" + "b" * 64
    candidate, report_path = tmp_path / "candidate.sqlite", tmp_path / "candidate-report.json"
    for index, generation, destination in [(cache / "index.sqlite", old, public), (candidate, new, staged)]:
        report = {"status": "validated", "generation": generation}
        with connect(index) as db:
            schema(db)
            db.executemany(
                "INSERT INTO metadata VALUES(?,?)",
                [
                    ("generation", json.dumps(generation)),
                    ("generatedAt", json.dumps("2026-10-27T00:00:00Z")),
                    ("reconciliation", json.dumps(report)),
                    (
                        "publication_reconciliation",
                        json.dumps({"currentTables": 1, "statuses": {"missing_provenance": 1}}),
                    ),
                ],
            )
            db.execute(
                "INSERT INTO inputs VALUES(?,?,?)",
                (str(raw / "snapshot.json"), digest(raw / "snapshot.json"), (raw / "snapshot.json").stat().st_size),
            )
            receipt = raw / "acquisitions/sha256/ab/receipt.json"
            db.execute("INSERT INTO inputs VALUES(?,?,?)", (str(receipt), digest(receipt), receipt.stat().st_size))
            db.execute("INSERT INTO collections VALUES('source','Source','{}')")
            db.execute("INSERT INTO files VALUES('file','source',NULL,'source.txt','receipt-only',NULL,NULL,NULL,'{}')")
            version = "sha256:" + "c" * 64
            table = {
                "id": "table",
                "family": "source",
                "status": "published",
                "artifact_digest": version,
                "publication": {
                    "scope": "current",
                    "version": version,
                    "status": "missing_provenance",
                    "sourceFiles": 0,
                    "matchedSourceFiles": 0,
                    "missingSourceFiles": 0,
                    "capturedSourceFiles": 0,
                    "inputClosureComplete": False,
                    "reasons": ["complete-input-connection-unproven"],
                },
            }
            db.execute("INSERT INTO tables VALUES('table','source','source',?)", (json.dumps(table),))
        export(index, destination)
        if generation == old:
            write(cache / "index.sqlite.reconciliation.json", report)
            write(cache / "reconciliation.json", report)
        else:
            write(report_path, report)
    raw_pins = selection.raw_selection(raw)
    members, _ = selection.public_members(public, old)
    write(
        cache / "selection.json",
        {"format": "spicygov-local-catalog-selection", "version": 1, "generation": old, **raw_pins, "files": members},
    )
    write(
        cache / "current.json",
        {
            "generation": old,
            **raw_pins,
            "index": "index.sqlite",
            "selection": "selection.json",
            "public_manifest": str(public / "manifest.json"),
            "public_manifest_sha256": digest(public / "manifest.json").removeprefix("sha256:"),
            "index_sha256": digest(cache / "index.sqlite").removeprefix("sha256:"),
        },
    )
    return dict(
        candidate=candidate,
        reconciliation=report_path,
        staged_public=staged,
        cache=cache,
        public=public,
        raw_root=raw,
        expected_generation=old,
    )


def selected_bytes(prepared):
    cache, public = prepared["cache"], prepared["public"]
    names = [
        cache / name
        for name in (
            "index.sqlite",
            "index.sqlite.reconciliation.json",
            "reconciliation.json",
            "selection.json",
            "current.json",
        )
    ]
    names.extend(path for path in public.rglob("*") if path.is_file())
    return {path: path.read_bytes() for path in names}


def test_selection_keeps_unresolved_history_and_updates_all_pointers(prepared, monkeypatch):
    candidate_bytes = prepared["candidate"].read_bytes()
    calls = []
    original = selection.digest

    def tracked(path):
        calls.append(path)
        return original(path)

    monkeypatch.setattr(selection, "digest", tracked)
    result = selection.select(**prepared)
    assert result["status"] == "selected"
    assert calls.count(prepared["candidate"]) == 1
    assert (prepared["cache"] / "index.sqlite").read_bytes() == candidate_bytes
    assert not prepared["candidate"].exists()
    assert Path(result["backup"], "recovery.json").is_file()
    for path in [
        prepared["cache"] / name
        for name in ("current.json", "selection.json", "reconciliation.json", "index.sqlite.reconciliation.json")
    ] + [prepared["public"] / "manifest.json"]:
        assert json.loads(path.read_text())["generation"] == result["generation"]
    selected = json.loads((prepared["cache"] / "selection.json").read_text())
    for item in selected["files"]:
        path = prepared["public"] / item["path"]
        assert digest(path) == item["sha256"] and path.stat().st_size == item["bytes"]


def test_select_cli_dispatches_documented_arguments(prepared, monkeypatch, capsys):
    from spicy_regs.source_catalog.__main__ import main

    arguments = ["source_catalog", "select"]
    for key, value in prepared.items():
        arguments.extend(["--" + key.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", arguments)
    main()
    assert json.loads(capsys.readouterr().out)["status"] == "selected"


@pytest.mark.parametrize(
    "fault",
    ["generation", "report", "raw", "page", "traversal", "extra", "publication", "snapshot", "receipt-set", "sidecar"],
)
def test_rejects_invalid_preparation_without_replacing_selected_files(prepared, fault):
    before = selected_bytes(prepared)
    if fault == "generation":
        prepared["expected_generation"] = "sha256:" + "c" * 64
    elif fault == "report":
        write(prepared["reconciliation"], {"status": "failed", "generation": "sha256:" + "b" * 64})
    elif fault == "raw":
        write(prepared["raw_root"] / "acquisitions/sha256/cd/new.json", {})
    elif fault in {"page", "traversal"}:
        path = prepared["staged_public"] / "manifest.json"
        value = json.loads(path.read_text())
        if fault == "page":
            detail = json.loads((prepared["staged_public"] / value["collections"][0]["detailPath"]).read_text())
            (prepared["staged_public"] / detail["files"]["pages"][0]["path"]).unlink()
        else:
            value["collections"][0]["detailPath"] = "../private.json"
            write(path, value)
    elif fault == "extra":
        write(prepared["staged_public"] / "generations" / ("b" * 64) / "unreferenced.json", {})
    elif fault == "sidecar":
        Path(str(prepared["candidate"]) + "-journal").touch()
    else:
        with connect(prepared["candidate"]) as db:
            if fault == "publication":
                db.execute("DELETE FROM metadata WHERE key='publication_reconciliation'")
            elif fault == "receipt-set":
                db.execute("DELETE FROM inputs WHERE path LIKE '%/acquisitions/%'")
            else:
                db.execute("DELETE FROM inputs")
    with pytest.raises(ValueError):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


def test_rejects_concurrent_selected_change(prepared, monkeypatch):
    original = selection.digest

    def changed(path):
        result = original(path)
        if path == prepared["candidate"]:
            current = prepared["cache"] / "current.json"
            current.write_text(current.read_text() + "\n")
        return result

    monkeypatch.setattr(selection, "digest", changed)
    with pytest.raises(ValueError, match="Selected files changed"):
        selection.select(**prepared)
    assert prepared["candidate"].is_file()


@pytest.mark.parametrize("missing", [False, True])
def test_staged_guard_reports_changed_path_and_identities(prepared, monkeypatch, missing):
    before = selected_bytes(prepared)
    path = prepared["staged_public"] / "manifest.json"
    expected = selection.identity(path)
    original = selection.digest

    def changed(candidate):
        result = original(candidate)
        if candidate == prepared["candidate"]:
            if missing:
                path.unlink()
            else:
                path.write_text(path.read_text() + "\n")
        return result

    monkeypatch.setattr(selection, "digest", changed)
    with pytest.raises(ValueError, match="Staged public files changed") as raised:
        selection.select(**prepared)
    message = str(raised.value)
    assert str(path) in message
    assert f"'bytes': {expected[2]}" in message
    assert "mtime_ns" in message and "ctime_ns" in message
    assert ("current unavailable" if missing else "current={") in message
    assert selected_bytes(prepared) == before
    assert prepared["candidate"].is_file()


@pytest.mark.parametrize("failure_at", range(1, 8))
def test_switch_failure_restores_previous_selection_and_candidate(prepared, monkeypatch, failure_at):
    before = selected_bytes(prepared)
    candidate = prepared["candidate"].read_bytes()
    original, calls = selection.os.replace, 0

    def fail_once(source, target):
        nonlocal calls
        calls += 1
        if calls == failure_at:
            raise OSError("injected switch failure")
        return original(source, target)

    monkeypatch.setattr(selection.os, "replace", fail_once)
    with pytest.raises(OSError, match="injected"):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before
    assert prepared["candidate"].read_bytes() == candidate
    assert not (prepared["public"] / "generations" / ("b" * 64)).exists()


def test_public_rollback_does_not_hardlink_across_filesystems(prepared, monkeypatch):
    before = selected_bytes(prepared)
    original_link, original_replace = selection.os.link, selection.os.replace

    def no_cross_device_link(source, target):
        if Path(target).parent == prepared["public"]:
            raise OSError("cross-device link")
        return original_link(source, target)

    def fail_current(source, target):
        if Path(target).name == "current.json" and Path(source).name.startswith("prepared-"):
            raise OSError("injected final pointer failure")
        return original_replace(source, target)

    monkeypatch.setattr(selection.os, "link", no_cross_device_link)
    monkeypatch.setattr(selection.os, "replace", fail_current)
    with pytest.raises(OSError, match="injected"):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


def append_acquisition(prepared, *, update_candidate=True, change_snapshot=True):
    """Acquire locally first; only a rebuilt candidate may select the new inventory."""
    raw = prepared["raw_root"]
    if change_snapshot:
        write(raw / "snapshot.json", {"snapshot": "retained-and-appended"})
    write(raw / "acquisitions/sha256/cd/new.json", {"receipt": "appended"})
    if update_candidate:
        with connect(prepared["candidate"]) as db:
            for path in (raw / "snapshot.json", raw / "acquisitions/sha256/cd/new.json"):
                db.execute(
                    "INSERT OR REPLACE INTO inputs VALUES(?,?,?)", (str(path), digest(path), path.stat().st_size)
                )


@pytest.mark.parametrize("change_snapshot", [False, True])
def test_reviewed_appended_inventory_updates_controls_and_retains_previous_pins(prepared, change_snapshot):
    old = json.loads((prepared["cache"] / "current.json").read_text())
    append_acquisition(prepared, change_snapshot=change_snapshot)
    raw = selection.raw_selection(prepared["raw_root"])
    result = selection.select(**prepared, allow_inventory_change=True)
    assert result["inventoryChanged"] is True
    assert result["rawInventory"] == raw
    assert result["previousRawInventory"] == {key: old[key] for key in raw}
    for name in ("current.json", "selection.json"):
        selected = json.loads((prepared["cache"] / name).read_text())
        assert {key: selected[key] for key in raw} == raw
    recovery = json.loads(Path(result["backup"], "recovery.json").read_text())
    assert recovery["previousRawInventory"] == result["previousRawInventory"]
    assert recovery["rawInventory"] == raw


@pytest.mark.parametrize("fault", ["snapshot", "receipt-set", "controls", "generation"])
def test_inventory_opt_in_still_rejects_stale_candidate_or_baseline(prepared, fault):
    append_acquisition(prepared)
    if fault in {"snapshot", "receipt-set"}:
        with connect(prepared["candidate"]) as db:
            if fault == "snapshot":
                db.execute(
                    "UPDATE inputs SET sha256=? WHERE path=?",
                    ("sha256:" + "a" * 64, str(prepared["raw_root"] / "snapshot.json")),
                )
            else:
                db.execute(
                    "DELETE FROM inputs WHERE path=?", (str(prepared["raw_root"] / "acquisitions/sha256/cd/new.json"),)
                )
    elif fault == "controls":
        path = prepared["cache"] / "selection.json"
        selected = json.loads(path.read_text())
        selected["raw_acquisition_receipts"] += 1
        write(path, selected)
    else:
        prepared["expected_generation"] = "sha256:" + "c" * 64
    before = selected_bytes(prepared)
    with pytest.raises(ValueError):
        selection.select(**prepared, allow_inventory_change=True)
    assert selected_bytes(prepared) == before
    assert prepared["candidate"].is_file()


@pytest.mark.parametrize("target", ["raw", "raw-snapshot", "candidate", "selected"])
def test_inventory_opt_in_rejects_concurrent_changes(prepared, monkeypatch, target):
    append_acquisition(prepared)
    before = selected_bytes(prepared)
    original = selection.digest

    def changed(path):
        result = original(path)
        if path == prepared["candidate"]:
            if target == "raw":
                write(prepared["raw_root"] / "acquisitions/sha256/ef/concurrent.json", {})
            elif target == "raw-snapshot":
                write(prepared["raw_root"] / "snapshot.json", {"snapshot": "concurrent"})
            elif target == "candidate":
                with connect(path) as db:
                    db.execute("INSERT INTO metadata VALUES('concurrent','true')")
            else:
                current = prepared["cache"] / "current.json"
                current.write_text(current.read_text() + "\n")
        return result

    monkeypatch.setattr(selection, "digest", changed)
    with pytest.raises(ValueError, match="changed during validation"):
        selection.select(**prepared, allow_inventory_change=True)
    if target != "selected":
        assert selected_bytes(prepared) == before
    assert prepared["candidate"].is_file()


@pytest.mark.parametrize("failure_at", range(1, 8))
def test_changed_inventory_switch_rolls_back_controls_without_reverting_raw(prepared, monkeypatch, failure_at):
    append_acquisition(prepared)
    before = selected_bytes(prepared)
    acquired = selection.raw_selection(prepared["raw_root"])
    candidate = prepared["candidate"].read_bytes()
    original, calls = selection.os.replace, 0

    def fail_once(source, target):
        nonlocal calls
        calls += 1
        if calls == failure_at:
            raise OSError("injected inventory switch failure")
        return original(source, target)

    monkeypatch.setattr(selection.os, "replace", fail_once)
    with pytest.raises(OSError, match="injected"):
        selection.select(**prepared, allow_inventory_change=True)
    assert selected_bytes(prepared) == before
    assert prepared["candidate"].read_bytes() == candidate
    assert selection.raw_selection(prepared["raw_root"]) == acquired


def test_select_cli_accepts_reviewed_inventory_flag(prepared, monkeypatch, capsys):
    from spicy_regs.source_catalog.__main__ import main

    append_acquisition(prepared)
    arguments = ["source_catalog", "select", "--allow-inventory-change"]
    for key, value in prepared.items():
        arguments.extend(["--" + key.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", arguments)
    main()
    assert json.loads(capsys.readouterr().out)["inventoryChanged"] is True


@pytest.mark.parametrize("fault", ["controls-invalid", "selected-index", "publication"])
def test_inventory_change_preserves_baseline_and_publication_guards(prepared, fault):
    append_acquisition(prepared)
    if fault == "controls-invalid":
        for name in ("current.json", "selection.json"):
            path = prepared["cache"] / name
            value = json.loads(path.read_text())
            value["raw_acquisition_receipt_set_sha256"] = None
            write(path, value)
    elif fault == "selected-index":
        with connect(prepared["cache"] / "index.sqlite") as db:
            db.execute("DELETE FROM inputs WHERE path LIKE '%/acquisitions/%'")
    else:
        with connect(prepared["candidate"]) as db:
            db.execute("DELETE FROM metadata WHERE key='publication_reconciliation'")
    before = selected_bytes(prepared)
    with pytest.raises(ValueError):
        selection.select(**prepared, allow_inventory_change=True)
    assert selected_bytes(prepared) == before


def public_version(prepared, version):
    root = prepared["staged_public"]
    manifest = json.loads((root / "manifest.json").read_text())
    for summary in manifest["collections"]:
        path = root / summary["detailPath"]
        detail = json.loads(path.read_text())
        periods = detail["collection"]["periods"]
        if version == 1:
            summary.pop("periodSummary", None)
            summary["periods"] = periods
        else:
            summary.pop("periods", None)
            summary["periodSummary"] = {"count": len(periods)}
        detail["collection"] = {**summary, "periods": periods}
        from tests.source_catalog.file_helpers import read_file_page

        for inventory in [detail["files"], *([detail["retainedFiles"]] if "retainedFiles" in detail else [])]:
            for index, part in enumerate(inventory["pages"]):
                source = root / part["path"]
                if source.suffix == ".parquet":
                    page = read_file_page(source)
                    page["version"] = version
                    relative = str(Path(part["path"]).with_suffix(".json"))
                    write(root / relative, page)
                    source.unlink()
                    inventory["pages"][index] = {"path": relative, "rows": part["rows"]}
        write(path, detail)
    write(root / "manifest.json", manifest)
    for path in root.rglob("*.json"):
        value = json.loads(path.read_text())
        value["version"] = version
        write(path, value)


@pytest.mark.parametrize("version", [1, 2])
def test_selection_supports_consistent_public_versions(prepared, version):
    public_version(prepared, version)
    assert selection.select(**prepared)["status"] == "selected"


@pytest.mark.parametrize("fault", ["mixed-version", "changed-summary"])
def test_selection_rejects_inconsistent_v2_public_export(prepared, fault):
    public_version(prepared, 2)
    manifest = json.loads((prepared["staged_public"] / "manifest.json").read_text())
    path = prepared["staged_public"] / manifest["collections"][0]["detailPath"]
    value = json.loads(path.read_text())
    if fault == "mixed-version":
        value["version"] = 1
    else:
        value["collection"]["title"] = "Unreviewed title"
    write(path, value)
    before = selected_bytes(prepared)
    with pytest.raises(ValueError):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


def test_updated_inventory_candidate_requires_explicit_opt_in(prepared):
    append_acquisition(prepared)
    before = selected_bytes(prepared)
    with pytest.raises(ValueError, match="--allow-inventory-change"):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


def test_selection_rejects_consistent_public_content_that_differs_from_candidate(prepared):
    before = selected_bytes(prepared)
    manifest_path = prepared['staged_public'] / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    summary = manifest['collections'][0]
    detail_path = prepared['staged_public'] / summary['detailPath']
    detail = json.loads(detail_path.read_text())
    summary['title'] = detail['collection']['title'] = 'Different source'
    for path, value in ((manifest_path, manifest), (detail_path, detail)):
        path.write_text(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')

    with pytest.raises(ValueError, match='candidate'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before
    assert prepared['candidate'].is_file()


@pytest.mark.parametrize('fault', ['manifest-metadata', 'aggregate', 'candidate-title', 'parquet-row'])
def test_rejects_candidate_and_public_content_mismatches(prepared, fault):
    import hashlib
    from spicy_regs.source_catalog.file_inventory import encode, validate

    before = selected_bytes(prepared)
    root = prepared['staged_public']
    manifest_path = root / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    detail_path = root / manifest['collections'][0]['detailPath']
    detail = json.loads(detail_path.read_text())
    if fault == 'manifest-metadata':
        manifest['generatedAt'] = '2000-01-01T00:00:00Z'
    elif fault == 'aggregate':
        manifest['totals']['aliases'] += 1
    elif fault == 'candidate-title':
        with connect(prepared['candidate']) as db:
            db.execute("UPDATE collections SET title='Changed candidate' WHERE id='source'")
    else:
        page = detail['files']['pages'][0]
        path = root / page['path']
        generation = manifest['generation']
        rows = validate(path.read_bytes(), generation=generation, dataset_id='source', inventory_kind='active')
        rows[0]['name'] = 'different.txt'
        body = encode(rows, generation=generation, dataset_id='source', inventory_kind='active')
        path.write_bytes(body)
        page['bytes'] = len(body)
        page['sha256'] = 'sha256:' + hashlib.sha256(body).hexdigest()
    for path, value in ((manifest_path, manifest), (detail_path, detail)):
        path.write_text(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')
    with pytest.raises(ValueError, match='candidate'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before
    assert prepared['candidate'].is_file()


def test_selection_uses_export_page_size(prepared, tmp_path):
    with connect(prepared['candidate']) as db:
        db.execute("INSERT INTO files VALUES('file2','source',NULL,'second.txt','receipt-only',NULL,NULL,NULL,'{}')")
    stage = tmp_path / 'small-pages'
    export(prepared['candidate'], stage, page_size=1)
    prepared['staged_public'] = stage
    assert selection.select(**prepared)['status'] == 'selected'


@pytest.mark.parametrize('version', [2, 3])
def test_selection_refuses_changed_pinned_receipt(prepared, tmp_path, version):
    receipt = tmp_path / 'receipts' / 'listing.json'
    write(receipt, {})
    facts = {'mapping': {'receipt_evidence_ref': {
        'manifest': 'receipts/listing.json', 'sha256': digest(receipt), 'entry': 1,
    }}}
    with connect(prepared['candidate']) as db:
        db.execute('INSERT INTO inputs VALUES(?,?,?)', (str(receipt), digest(receipt), receipt.stat().st_size))
        db.execute("UPDATE files SET data=? WHERE id='file'", (json.dumps(facts),))
    stage = tmp_path / 'receipt-stage'
    export(prepared['candidate'], stage)
    prepared['staged_public'] = stage
    if version == 2:
        public_version(prepared, 2)
    before = selected_bytes(prepared)
    receipt.write_text('[]')
    with pytest.raises(ValueError, match='Pinned receipt bytes changed'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before
    assert prepared['candidate'].is_file()


@pytest.mark.parametrize('target', ['candidate', 'staged-member'])
def test_selection_refuses_linked_inputs(prepared, tmp_path, target):
    before = selected_bytes(prepared)
    if target == 'candidate':
        candidate = prepared['candidate']
        link = tmp_path / 'candidate-link.sqlite'
        link.symlink_to(candidate)
        prepared['candidate'] = link
    else:
        root = prepared['staged_public']
        manifest = json.loads((root / 'manifest.json').read_text())
        path = root / manifest['collections'][0]['detailPath']
        data = path.read_bytes()
        actual = tmp_path / 'linked-detail.json'
        actual.write_bytes(data)
        path.unlink()
        path.symlink_to(actual)
    with pytest.raises(ValueError, match='Linked path'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


@pytest.mark.parametrize('version', [1, 2])
def test_legacy_export_must_match_candidate(prepared, version):
    public_version(prepared, version)
    root = prepared['staged_public']
    manifest = json.loads((root / 'manifest.json').read_text())
    detail_path = root / manifest['collections'][0]['detailPath']
    detail = json.loads(detail_path.read_text())
    page_path = root / detail['files']['pages'][0]['path']
    page = json.loads(page_path.read_text())
    page['files'][0]['name'] = 'different.txt'
    write(page_path, page)
    before = selected_bytes(prepared)
    with pytest.raises(ValueError, match='candidate'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


def test_original_version_one_without_provenance_is_supported(prepared):
    public_version(prepared, 1)
    root = prepared['staged_public']
    manifest = json.loads((root / 'manifest.json').read_text())
    detail = json.loads((root / manifest['collections'][0]['detailPath']).read_text())
    path = root / detail['files']['pages'][0]['path']
    page = json.loads(path.read_text())
    for row in page['files']:
        row.pop('provenance')
    write(path, page)
    assert selection.select(**prepared)['status'] == 'selected'


def test_version_two_cannot_omit_provenance(prepared):
    public_version(prepared, 2)
    root = prepared['staged_public']
    manifest = json.loads((root / 'manifest.json').read_text())
    detail = json.loads((root / manifest['collections'][0]['detailPath']).read_text())
    path = root / detail['files']['pages'][0]['path']
    page = json.loads(path.read_text())
    for row in page['files']:
        row.pop('provenance')
    write(path, page)
    before = selected_bytes(prepared)
    with pytest.raises(ValueError, match='provenance is missing'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before


def test_preexisting_selected_noncontrol_mutation_refuses_before_any_switch(prepared):
    with connect(prepared['cache'] / 'index.sqlite') as db:
        db.execute("UPDATE collections SET title='Preexisting changed bytes'")
    before = selected_bytes(prepared)
    with pytest.raises(ValueError, match='recorded identity'):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before
    assert prepared['candidate'].is_file()


def test_full_build_cannot_replace_selected_index_or_report(prepared):
    from spicy_regs.source_catalog.importer import build

    before = selected_bytes(prepared)
    with pytest.raises(ValueError, match='selected catalog'):
        build(prepared['raw_root'], prepared['cache'] / 'index.sqlite')
    assert selected_bytes(prepared) == before


def admit_export(prepared):
    from spicy_regs.source_catalog.export_completion import export_completed

    prepared['staged_public'] = prepared['staged_public'].with_name('owned-public')
    return export_completed(prepared['candidate'], prepared['staged_public'], prepared['cache'])


def test_owned_export_survives_lost_response_and_selection_does_not_replay(prepared, monkeypatch):
    from spicy_regs.source_catalog import export_completion, exporter, file_inventory

    manifest, members = admit_export(prepared)
    def repeated(*args, **kwargs):
        raise AssertionError('Repeated export or Parquet validation')
    monkeypatch.setattr(exporter, 'export', repeated)
    monkeypatch.setattr(selection, 'export', repeated)
    monkeypatch.setattr(file_inventory, 'validate', repeated)
    # A new invocation reads durable completion, not in-memory exporter state.
    assert export_completion.export_completed(prepared['candidate'], prepared['staged_public'], prepared['cache']) == (manifest, members)
    original_digest = export_completion.digest
    def no_candidate_rehash(path):
        assert path != prepared['candidate'], 'Candidate already qualified by export'
        return original_digest(path)
    monkeypatch.setattr(export_completion, 'digest', no_candidate_rehash)
    selected = selection.select(**prepared)
    assert selected['status'] == 'selected'
    assert selection.read(prepared['cache'] / 'selection.json')['files'] == members


@pytest.mark.parametrize('mutation', ['candidate', 'member', 'extra', 'sidecar', 'completion', 'completion-hash', 'implementation', 'symlink'])
def test_owned_export_refuses_mutation_without_replay(prepared, monkeypatch, mutation):
    from spicy_regs.source_catalog import export_completion

    _, members = admit_export(prepared)
    baseline = selected_bytes(prepared)
    def repeated(*args, **kwargs):
        raise AssertionError('Invalid completion must not fall back to replay')
    monkeypatch.setattr(selection, 'export', repeated)
    if mutation == 'candidate':
        with connect(prepared['candidate']) as db:
            db.execute("UPDATE files SET name='changed'")
    elif mutation == 'member':
        member = next(item for item in members if item['path'].endswith('.parquet'))
        path = prepared['staged_public'] / member['path']
        body = bytearray(path.read_bytes())
        body[-1] ^= 1
        path.write_bytes(body)
    elif mutation == 'extra':
        path = prepared['staged_public'] / next(item['path'] for item in members if item['path'].endswith('.parquet'))
        path.with_name('extra').write_text('unexpected')
    elif mutation == 'sidecar':
        prepared['candidate'].with_name(prepared['candidate'].name+'-journal').touch()
    elif mutation == 'completion':
        export_completion.completion_path(prepared['cache'], prepared['candidate'], prepared['staged_public']).write_text('{}')
    elif mutation == 'completion-hash':
        path = export_completion.completion_path(prepared['cache'], prepared['candidate'], prepared['staged_public'])
        record = selection.read(path)
        sha = record['payload']['indexSha256']
        record['payload']['indexSha256'] = sha[:-1] + ('0' if sha[-1] != '0' else '1')
        write(path, record)
    elif mutation == 'implementation':
        monkeypatch.setattr(export_completion, 'implementation', lambda: {'changed': True})
    else:
        path = prepared['staged_public'] / 'manifest.json'
        moved = path.with_name('real-manifest.json')
        path.rename(moved)
        path.symlink_to(moved)
    with pytest.raises(ValueError):
        selection.select(**prepared)
    assert selected_bytes(prepared) == baseline


@pytest.mark.parametrize('missing', [False, True])
def test_owned_export_preserves_positive_and_negative_reference_decisions(prepared, monkeypatch, missing):
    receipt = prepared['raw_root'] / 'retained/receipt.json'
    write(receipt, {'retained': True})
    body = receipt.read_bytes()
    facts = {'mapping': {'receipt_evidence_ref': {
        'manifest': str(receipt.relative_to(prepared['raw_root'])), 'sha256': digest(receipt), 'entry': 1}}}
    with connect(prepared['candidate']) as db:
        db.execute('INSERT INTO inputs VALUES(?,?,?)', (str(receipt), digest(receipt), len(body)))
        db.execute('UPDATE files SET data=?', (json.dumps(facts),))
    if missing:
        receipt.unlink()
    admit_export(prepared)
    receipt.write_bytes(body if missing else b'changed receipt')
    monkeypatch.setattr(selection, 'export', lambda *a, **k: pytest.fail('Repeated export'))
    with pytest.raises(ValueError, match='Export reference changed'):
        selection.select(**prepared)


def test_export_cli_lost_response_recovers_in_fresh_process(prepared):
    import subprocess
    import sys

    subprocess.run([sys.executable, '-m', 'spicy_regs.source_catalog', 'export',
                    '--index', str(prepared['candidate']), '--output', str(prepared['staged_public']),
                    '--completion-cache', str(prepared['cache'])], check=True, capture_output=True)
    script = '''
import sys
from pathlib import Path
from spicy_regs.source_catalog import exporter, file_inventory
from spicy_regs.source_catalog.export_completion import export_completed
def repeated(*args, **kwargs):
    raise AssertionError('Completed export replayed in fresh process')
exporter.export = repeated
file_inventory.validate = repeated
manifest, members = export_completed(*map(Path, sys.argv[1:]))
assert manifest['generation'] and members
'''
    subprocess.run([sys.executable, '-c', script, str(prepared['candidate']),
                    str(prepared['staged_public']), str(prepared['cache'])], check=True, capture_output=True)


def test_original_reference_completion_captures_compressed_storage_preference(prepared):
    import gzip
    import hashlib
    from spicy_regs.source_catalog.export_completion import check_references
    from spicy_regs.source_catalog.file_inventory import ReceiptReferences

    body = b'{"original-receipt":true}'
    sha = hashlib.sha256(body).hexdigest()
    key = f'objects/sha256/{sha[:2]}/{sha}'
    raw = prepared['raw_root']
    packed = raw / (key + '.gz')
    packed.parent.mkdir(parents=True)
    packed.write_bytes(gzip.compress(body))
    dependencies = {}
    with connect(prepared['candidate']) as db:
        db.execute('CREATE TABLE raw_objects(sha256 TEXT,object TEXT,present INTEGER)')
        db.execute('INSERT INTO objects VALUES(?,?)', ('sha256:' + sha, len(body)))
        db.execute('INSERT INTO raw_objects VALUES(?,?,1)', ('sha256:' + sha, key + '.gz'))
        refs = ReceiptReferences(db, dependencies)
        assert refs.original('sha256:' + sha) == key + '.gz'
    assert dependencies[str(raw / key)] is None
    check_references(dependencies)
    # The raw owner would now choose this uncompressed member first, even
    # though the compressed member has exactly the same identity as before.
    (raw / key).write_bytes(body)
    with pytest.raises(ValueError, match='Export reference changed'):
        check_references(dependencies)
