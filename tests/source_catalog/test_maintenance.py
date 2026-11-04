"""Metadata refresh produces a guarded candidate without rebuilding raw holdings."""

import hashlib
import json
from typing import Any
from pathlib import Path

import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog import maintenance
from spicy_regs.source_catalog.raw_archive import holdings
from spicy_regs.source_catalog.storage import connect, digest
from tests.source_catalog.test_raw_archive import base_archive


def write(path, value):
    path.write_text(json.dumps(value))


@pytest.fixture
def prepared(tmp_path):
    raw = tmp_path / "raw"
    base_archive(raw)
    body = json.dumps({"version": 1, "bulk_group": "Contributions", "member": {"name": "itcont.txt"},
                       "parsing": {"state": "unparsed"}, "coverage_limits": ["Selected member only."]}).encode()
    sha = hashlib.sha256(body).hexdigest()
    artifact, member = "a" * 64, f"blobs/sha256/{sha}"
    store = CentralRawArchiveStore(raw)
    store.put_blob(sha, len(body), [body])
    store.record_receipt(files=[{
        "sha256": sha, "bytes": len(body), "name": "context",
        "original_location": f"/old/source-evidence/{artifact}/{member}",
        "receipt_context": {"artifact": "sha256:" + artifact, "declared_role": "evidence",
                            "family": "fec-query", "member": member},
    }], receipt={"status": "captured"})
    config = tmp_path / "registry.json"
    configuration: dict[str, Any] = {
        "collections": [{"id": "family:bill-family", "title": "Before", "families": ["bill-family"]}],
        "routes": [{"id": "bill-route", "collection_id": "family:bill-family", "description": "Before"}],
        "periods": [{"id": "configured-period", "collection_id": "family:bill-family", "kind": "congress", "label": "119"}],
    }
    write(config, configuration)
    index = tmp_path / "base.sqlite"
    report = build(raw, index, config=config)
    with connect(index) as db:
        data = json.loads(db.execute("SELECT data FROM collections WHERE id='family:bill-family'").fetchone()[0])
        db.execute("UPDATE collections SET data=? WHERE id='family:bill-family'", (json.dumps(dict(data, imported_fact="preserved")),))
        db.execute("INSERT INTO evidence VALUES('historical-observation','family:bill-family','periods',?)", (
            json.dumps({"kind": "capture-date", "label": "2026-10-24"}),
        ))
        # Reproduce an older index that retained generated context as a held
        # source, before the maintained bounded classification was available.
        row = db.execute("SELECT id,data FROM files WHERE name='context'").fetchone()
        facts = json.loads(row["data"])
        facts["mapping"].pop("content_metadata", None)
        facts["mapping"]["evidence"] = [item for item in facts["mapping"].get("evidence", []) if item.get("origin") != "content"]
        db.execute("UPDATE files SET status='held',data=? WHERE id=?", (json.dumps(facts), row["id"]))
    configuration["collections"][0].update(title="After", description="Reviewed source description")
    configuration["routes"][0]["description"] = "After"
    configuration["periods"][0]["label"] = "119th Congress"
    configuration["catalog_scope"] = {"format": "spicy-source-scope", "version": 1, "excluded_objects": [],
                                      "collections": [{"id": "family:bill-family", "disposition": "active", "reason": "Reviewed"},
                                                      {"id": "family:fec-query", "disposition": "retained", "reason": "Evidence"}]}
    write(config, configuration)
    return dict(index=index, output=tmp_path / "candidate.sqlite", config=config,
                expected_generation=report["generation"])


def raw_bytes(prepared):
    return {path: path.read_bytes() for path in (prepared["index"].parent / "raw").rglob("*") if path.is_file()}


def test_refresh_matches_fresh_import_for_classification_and_configured_rows(prepared):
    before, raw_before = prepared["index"].read_bytes(), raw_bytes(prepared)
    result = maintenance.refresh_metadata(**prepared)
    fresh = prepared["output"].parent / "fresh.sqlite"
    build(prepared["index"].parent / "raw", fresh, config=prepared["config"])
    assert prepared["index"].read_bytes() == before
    assert raw_bytes(prepared) == raw_before
    assert result["generation"] != result["baseGeneration"] == prepared["expected_generation"]
    assert result["status"] == "validated"
    with connect(prepared["output"], readonly=True, scope="all") as db, connect(fresh, readonly=True, scope="all") as expected:
        assert [tuple(row) for row in db.execute("SELECT id,status,data FROM files ORDER BY id")] == [
            tuple(row) for row in expected.execute("SELECT id,status,data FROM files ORDER BY id")]
        assert db.execute("SELECT title FROM collections WHERE id='family:bill-family'").fetchone()[0] == "After"
        data = json.loads(db.execute("SELECT data FROM collections WHERE id='family:bill-family'").fetchone()[0])
        assert data["imported_fact"] == "preserved" and data["families"] == ["bill-family"]
        assert db.execute("SELECT data FROM routes WHERE id='bill-route'").fetchone()[0] == expected.execute(
            "SELECT data FROM routes WHERE id='bill-route'").fetchone()[0]
        assert db.execute("SELECT data FROM evidence WHERE id='configured-period'").fetchone()[0] == expected.execute(
            "SELECT data FROM evidence WHERE id='configured-period'").fetchone()[0]
        assert db.execute("SELECT count(*) FROM evidence WHERE id='historical-observation'").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM evidence WHERE kind='configured_metadata_history'").fetchone()[0] == 3
        assert {key: value for key, value in result["raw_archive"].items() if key != "inventory_scope"} == holdings(db)
        assert json.loads(db.execute("SELECT value FROM metadata WHERE key='reconciliation'").fetchone()[0]) == result
        assert str(prepared["config"]) in result["phaseInputs"]
        assert result["validation"]["fullInheritedIndexScan"] is False
        from spicy_regs.source_catalog.raw_classification import classification_inputs
        assert all(str(path.resolve()) in result["phaseInputs"] for path in classification_inputs())
    report = prepared["output"].with_suffix(".sqlite.reconciliation.json")
    assert json.loads(report.read_text()) == result


def test_refresh_remaps_inherited_rows_when_object_datasets_binds_digest(prepared):
    with connect(prepared["index"], readonly=True, scope="all") as source:
        sha = source.execute("SELECT sha256 FROM files WHERE name='context'").fetchone()[0]
    configuration = json.loads(prepared["config"].read_text())
    configuration["collections"].append({
        "id": "dataset:fec-research-source-pages",
        "title": "FEC source documentation pages",
    })
    configuration["catalog_scope"]["collections"].append({
        "id": "dataset:fec-research-source-pages",
        "disposition": "retained",
        "reason": "Evidence",
    })
    configuration["object_datasets"] = [{
        "dataset_id": "dataset:fec-research-source-pages",
        "family": "fec-query",
        "sha256": [sha.removeprefix("sha256:")],
    }]
    write(prepared["config"], configuration)

    result = maintenance.refresh_metadata(**prepared)
    fresh = prepared["output"].parent / "fresh.sqlite"
    build(prepared["index"].parent / "raw", fresh, config=prepared["config"])

    assert result["configuredMetadata"]["object_datasets"] == 1
    with connect(prepared["output"], readonly=True, scope="all") as db, connect(fresh, readonly=True, scope="all") as expected:
        row = db.execute("SELECT collection_id,status,data FROM files WHERE name='context'").fetchone()
        assert row["collection_id"] == "dataset:fec-research-source-pages"
        assert row["status"] == "supporting-evidence"
        mapping = json.loads(row["data"])["mapping"]
        assert mapping["dataset_id"] == "dataset:fec-research-source-pages"
        assert mapping["candidate_dataset_ids"] == ["dataset:fec-research-source-pages"]
        assert [tuple(r) for r in db.execute("SELECT id,collection_id,status,data FROM files ORDER BY id")] == [
            tuple(r) for r in expected.execute("SELECT id,collection_id,status,data FROM files ORDER BY id")
        ]



def test_unchanged_refresh_reuses_accepted_generation_without_cloning_or_reading_sources(prepared, monkeypatch):
    result = maintenance.refresh_metadata(**prepared)
    second: dict[str, Any] = dict(prepared, index=prepared['output'], output=prepared['output'].with_name('unchanged.sqlite'),
                  expected_generation=result['generation'], reuse_unchanged=True)
    def forbidden(*args, **kwargs):
        raise AssertionError('Unchanged refresh repeated an expensive phase')
    monkeypatch.setattr(maintenance, '_clone_index', forbidden)
    monkeypatch.setattr(maintenance, 'classify_retained_objects', forbidden)
    monkeypatch.setattr(maintenance, 'configured_metadata', forbidden)
    reused = maintenance.refresh_metadata(**second)
    assert reused['status'] == 'unchanged' and reused['generation'] == result['generation']
    assert not second['output'].exists()


@pytest.mark.parametrize("fault", ["generation", "raw", "inside-raw", "same-index", "existing-output", "sidecar"])
def test_refresh_refuses_invalid_base_or_output_without_mutating_source(prepared, fault):
    if fault == "generation":
        prepared["expected_generation"] = "sha256:" + "f" * 64
    elif fault == "raw":
        snapshot = prepared["index"].parent / "raw/snapshot.json"
        snapshot.write_text(snapshot.read_text() + "\n")
    elif fault == "inside-raw":
        prepared["output"] = prepared["index"].parent / "raw/candidate.sqlite"
    elif fault == "same-index":
        prepared["output"] = prepared["index"]
    elif fault == "sidecar":
        Path(str(prepared["index"]) + "-journal").touch()
    else:
        prepared["output"].write_bytes(b"another candidate")
    before, raw_before = prepared["index"].read_bytes(), raw_bytes(prepared)
    with pytest.raises(ValueError):
        maintenance.refresh_metadata(**prepared)
    assert prepared["index"].read_bytes() == before
    assert raw_bytes(prepared) == raw_before
    if fault == "existing-output":
        assert prepared["output"].read_bytes() == b"another candidate"


@pytest.mark.parametrize("target", ["raw", "source", "config", "output"])
def test_refresh_rejects_concurrent_changes(prepared, monkeypatch, target):
    original = maintenance.configured_metadata
    def changed(*args):
        result = original(*args)
        if target == "raw":
            snapshot = prepared["index"].parent / "raw/snapshot.json"
            snapshot.write_text(snapshot.read_text() + "\n")
        elif target == "source":
            replacement = prepared["index"].with_suffix(".replacement")
            replacement.write_bytes(prepared["index"].read_bytes())
            replacement.replace(prepared["index"])
        elif target == "config":
            prepared["config"].write_text(prepared["config"].read_text() + "\n")
        else:
            prepared["output"].write_bytes(b"concurrent candidate")
        return result
    monkeypatch.setattr(maintenance, "configured_metadata", changed)
    before = prepared["index"].read_bytes()
    with pytest.raises((ValueError, RuntimeError, FileExistsError)):
        maintenance.refresh_metadata(**prepared)
    assert prepared["index"].read_bytes() == before
    if target == "output":
        assert prepared["output"].read_bytes() == b"concurrent candidate"
    else:
        assert not prepared["output"].exists()
    assert not list(prepared["index"].parent.glob("candidate.sqlite.*.tmp"))


def test_refresh_cli_dispatches_maintained_candidate_command(prepared, monkeypatch, capsys):
    from spicy_regs.source_catalog.__main__ import main

    arguments = ["source_catalog", "refresh-metadata"]
    for key, value in prepared.items():
        arguments.extend(["--" + key.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", arguments)
    main()
    assert json.loads(capsys.readouterr().out)["status"] == "validated"


def publication_selection(prepared):
    root = prepared["index"].parent
    version = "sha256:" + "a" * 64
    publication = {"format": "spicy-regs-publication", "version": 1, "families": {"bill-family": {
        "prefix": "generations/bill-family/" + "a" * 64, "logicalId": "urn:test", "artifactDigest": version,
        "tables": {"bills.parquet": {"sha256": "sha256:" + "b" * 64, "byteSize": 4, "rows": 1,
                                     "columns": [["id", "VARCHAR"]]}},
    }}}
    path = root / "publication.json"
    write(path, publication)
    selected = root / "publication-selection.json"
    write(selected, {"format": "spicy-publication-reconciliation", "version": 1, "receipts": [],
                     "publication": {"path": path.name, "sha256": digest(path), "bytes": path.stat().st_size}})
    return selected


def test_refresh_composes_with_publication_and_preserves_base_input_pins(prepared):
    from spicy_regs.source_catalog.publication import check_publication

    selected = publication_selection(prepared)
    result = maintenance.refresh_metadata(**prepared, publication_reconciliation=selected)
    assert result["publication"]["currentTables"] == 1
    assert check_publication(prepared["output"], selected)["selectionMatches"] is True
    with connect(prepared["index"], readonly=True, scope="all") as source, connect(prepared["output"], readonly=True, scope="all") as candidate:
        assert [tuple(row) for row in source.execute("SELECT * FROM inputs ORDER BY path")] == [
            tuple(row) for row in candidate.execute("SELECT * FROM inputs ORDER BY path")]
        publication = json.loads(candidate.execute("SELECT value FROM metadata WHERE key='publication_reconciliation'").fetchone()[0])
        assert publication["generation"] == result["generation"]
        assert publication["currentTables"] == 1
    next_output = prepared["output"].with_name("next.sqlite")
    with pytest.raises(ValueError, match="requires --publication-reconciliation"):
        maintenance.refresh_metadata(prepared["output"], next_output, config=prepared["config"], expected_generation=result["generation"])
    assert not next_output.exists()


def test_publication_input_change_is_refused_before_candidate_is_exposed(prepared, monkeypatch):
    selected = publication_selection(prepared)
    original = maintenance.import_publications
    def changed(*args):
        result = original(*args)
        selected.write_text(selected.read_text() + "\n")
        return result
    monkeypatch.setattr(maintenance, "import_publications", changed)
    with pytest.raises(RuntimeError, match="Input changed"):
        maintenance.refresh_metadata(**prepared, publication_reconciliation=selected)
    assert not prepared["output"].exists()


def test_failed_report_install_removes_only_its_new_candidate(prepared, monkeypatch):
    report = prepared["output"].with_suffix(".sqlite.reconciliation.json")
    original = maintenance.os.link
    def concurrent_report(source, target):
        if target == report:
            report.write_bytes(b"concurrent report")
        return original(source, target)
    monkeypatch.setattr(maintenance.os, "link", concurrent_report)
    with pytest.raises(FileExistsError):
        maintenance.refresh_metadata(**prepared)
    assert not prepared["output"].exists()
    assert report.read_bytes() == b"concurrent report"


def test_old_layout_migration_preserves_rows_and_checks_new_index(prepared, capsys):
    with connect(prepared['index']) as db:
        db.execute('DROP INDEX files_holdings')
    before = prepared['index'].read_bytes()
    result = maintenance.refresh_metadata(**prepared)
    assert prepared['index'].read_bytes() == before
    assert result['storageLayout']['filesHoldingsIndexCreated'] is True
    assert 'files' in result['validation']['sqliteQuickCheckTables']
    assert result['validation']['fullInheritedIndexScan'] is True
    with connect(prepared['output']) as db:
        from spicy_regs.source_catalog.storage import summary_index
        assert summary_index(db) is False
        assert [row[2] for row in db.execute('PRAGMA index_info(files_holdings)')] == [
            'collection_id', 'status', 'sha256', 'id']
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='files_collection'").fetchone()
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines() if line.startswith('{"catalogPhase"')]
    for phase in ('summary-index', 'holdings-all', 'holdings-active', 'quick-check:files'):
        assert [event['status'] for event in events if event['catalogPhase'] == phase] == ['started', 'completed']
