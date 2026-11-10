"""Release selections refuse drift, omitted siblings and private receipt paths."""

import importlib.util
from functools import partial
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import DatasetPolicy


REGISTERED_RELEASE_DATASETS = (
    "administration_policy_bill_links",
    "administration_policy_statements",
    "ecfr_snapshot_sections",
    "ecfr_snapshots",
    "federal_register_issues",
    "table3_historical_records",
    "uscode_edition_structure",
    "uscode_editions",
    "uscode_edition_sections",
)


def test_declaration_refresh_preserves_installed_policies_and_shared_log_markers():
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.subject_catalog import (
        RECEIPTS_RETIRED, SHARED_LOG, descriptor_of, retired_receipt_datasets, shared_receipt_logs,
    )

    path = Path(__file__).parents[1] / "scripts/update_etl_policy_declarations.py"
    module_spec = importlib.util.spec_from_file_location("release_policy_declarations", path)
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    declared = module.declarations()
    for name, policy in installed_policies().items():
        assert descriptor_of(declared[name]) == policy.descriptor()
        # A declaration is its policy's descriptor plus exactly the markers it carries, which the generator keeps.
        markers = {SHARED_LOG} & {k for k, v in declared[name].items() if v is True} | (
            {RECEIPTS_RETIRED} if name in retired_receipt_datasets() else set())
        assert declared[name].keys() - policy.descriptor().keys() == markers
    assert {name for name, policy in declared.items() if policy.get(SHARED_LOG)} == set(shared_receipt_logs())
    assert {name for name, policy in declared.items() if policy.get(RECEIPTS_RETIRED)} == retired_receipt_datasets()
    assert "uscode_edition_sections" in declared


@pytest.mark.parametrize("datasets", [frozenset(), frozenset({"not_a_dataset"}), frozenset({"uscode_edition_sections", "not_a_dataset"})])
def test_selected_policy_refresh_refuses_empty_or_unknown_selection(datasets):
    path = Path(__file__).parents[1] / "scripts/update_etl_policy_declarations.py"
    spec = importlib.util.spec_from_file_location("selected_policy_declarations", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(ValueError):
        module.declarations(datasets)


def test_selected_section_policy_refresh_matches_maintained_declaration():
    from spicy_regs.sources.uscode_editions import POLICIES
    path = Path(__file__).parents[1] / "scripts/update_etl_policy_declarations.py"
    spec = importlib.util.spec_from_file_location("selected_policy_declarations", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.declarations(frozenset({"uscode_edition_sections"})) == {
        "uscode_edition_sections": POLICIES["uscode_edition_sections"].descriptor(),
    }


def test_cfr_declaration_uses_public_owner_without_changing_local_rebuild_policy():
    from scripts.update_etl_policy_declarations import declarations
    from spicy_regs.transforms.historical_regulations import POLICIES, PUBLIC_POLICIES
    from spicy_regs.transforms.regulations_receipts import policy

    public = policy("cfr_sections").descriptor()
    retained = POLICIES["cfr_sections"].descriptor()
    assert declarations(frozenset({"cfr_sections"})) == {"cfr_sections": public}
    assert "cfr_sections" not in PUBLIC_POLICIES
    assert retained["policy_version"] == "retained-annual-cfr-v1"
    assert retained != public


@pytest.mark.parametrize("dataset", REGISTERED_RELEASE_DATASETS)
def test_registered_release_policy_matches_final_producer(dataset):
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.sources import retained_collections, table3_history, uscode_editions
    from spicy_regs.transforms import historical_regulations

    producers = {
        **retained_collections.POLICIES,
        **historical_regulations.POLICIES,
        **uscode_editions.POLICIES,
        table3_history.POLICY.dataset: table3_history.POLICY,
    }
    assert installed_policies()[dataset].descriptor() == producers[dataset].descriptor()


@pytest.mark.parametrize("dataset", REGISTERED_RELEASE_DATASETS)
def test_registered_release_artifact_admission_requires_matching_receipts(tmp_path, dataset):
    from dataclasses import replace

    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.etl_receipts import ReceiptContext, write_dataset
    from spicy_regs.generations import build_generation, verify_generation

    policy = installed_policies()[dataset]
    # A small synthetic row tests admission, not source coverage or qualification.
    row: dict[str, object] = {field.name: (False if pa.types.is_boolean(field.type)
                        else 1 if pa.types.is_integer(field.type) else "fixture")
           for field in policy.subject_schema}
    row.update({field: {"fixture": "processing"} for field in policy.receipt_fields})
    context = ReceiptContext("test-generation", "test-attempt", "test-processor", [{
        "source_id": "test-source", "sha256": "sha256:" + "a" * 64,
        "source_uri": "https://publisher.example/test", "locator": "record:1",
    }])
    subject, receipts = write_dataset([(row, context)], tmp_path / "source", policy)
    assert subject is not None
    build = partial(build_generation, family="test-release", files=[subject], expected_keys=[subject.name])
    with pytest.raises(ValueError, match="require ETL receipts"):
        build(tmp_path / "missing-receipts")
    with pytest.raises(ValueError, match="differs from installed field policy"):
        build(tmp_path / "stale-policy", receipt_path=receipts,
                         receipt_policies=[replace(policy, policy_version="stale")],
                         receipt_generation_id=context.generation_id)
    artifact = build(tmp_path / "accepted", receipt_path=receipts,
                                receipt_policies=[policy],
                                receipt_generation_id=context.generation_id)
    assert verify_generation(tmp_path / "accepted").pin == artifact.pin
    changed = pq.read_table(subject).to_pylist()
    changed[0][policy.identity_fields[0]] = "different-identity"
    pq.write_table(pa.Table.from_pylist(changed, schema=policy.subject_schema), subject)
    with pytest.raises(ValueError, match="Missing, ambiguous or reused subject receipt"):
        build(tmp_path / "wrong-binding", receipt_path=receipts,
                         receipt_policies=[policy], receipt_generation_id=context.generation_id)


def test_registration_does_not_make_unpublished_tables_available(monkeypatch):
    import duckdb

    from spicy_regs import mcp_server
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.subject_catalog import subject_tables

    registered = installed_policies()
    assert set(REGISTERED_RELEASE_DATASETS) <= set(subject_tables(()))
    assert "uscode_edition_sections" in registered
    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    monkeypatch.setattr(mcp_server, "_connection_index", lambda _: {"families": {}})
    monkeypatch.setattr(mcp_server, "_connection_rulemaking", lambda _: {"tables": {}})
    monkeypatch.setattr(mcp_server, "_connection_relationships", lambda _: {})
    with duckdb.connect() as con:
        assert not set(REGISTERED_RELEASE_DATASETS) & set(mcp_server._available_tables(con))
        con.execute('CREATE TABLE "ecfr_snapshots" (snapshot_id VARCHAR)')
        assert set(REGISTERED_RELEASE_DATASETS) & set(mcp_server._available_tables(con)) == {"ecfr_snapshots"}


def test_unpublished_registered_tables_skip_bare_fallback(monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.sources import publication
    from spicy_regs.relationship_views.reference import REFERENCE_TABLES
    from tests.test_generation_mcp import connection_fixture

    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    built = connection_fixture(monkeypatch, publication.empty_index(), {},
                               tables=REGISTERED_RELEASE_DATASETS)
    mcp_server._build_connection()
    con, = built
    try:
        assert mcp_server._publication_status(con.cursor())["tables"] == list(REFERENCE_TABLES)
        assert con.seen == []
    finally:
        con.close()


def test_unpublished_registered_table_never_requests_failing_bare_key(monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.sources import publication
    from tests.test_generation_mcp import connection_fixture

    from spicy_regs.relationship_views.reference import REFERENCE_TABLES

    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    name = REGISTERED_RELEASE_DATASETS[0]
    built = connection_fixture(monkeypatch, publication.empty_index(), {},
                               tables=(name,), statuses={name + ".parquet": 503})
    mcp_server._build_connection()
    try:
        assert built[0].seen == []
        assert mcp_server._available_tables(built[0].cursor()) == list(REFERENCE_TABLES)
    finally:
        built[0].close()

spec = importlib.util.spec_from_file_location("release_preflight", Path(__file__).parents[1] / "scripts/prepare_source_catalog_release.py")
assert spec is not None and spec.loader is not None
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def pin(path):
    import hashlib
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def selection(tmp_path, monkeypatch, *, source_uri="https://publisher.example/source"):
    schema = pa.schema([("id", pa.string())])
    policy = DatasetPolicy("sample", schema, ("id",), ())
    monkeypatch.setattr(release, "installed_policies", lambda: {"sample": policy})
    monkeypatch.setattr(release, "implementation_id", lambda: "implementation:test")
    monkeypatch.setattr(release, "spicy_docs_code", lambda: "docs:test")
    subject = tmp_path / "sample.parquet"
    pq.write_table(pa.table({"id": ["one"]}), subject)
    receipt = tmp_path / "etl_receipts.parquet"
    pq.write_table(pa.table({"dataset": ["sample"], "generation_id": ["generation:test"], "source_uri": [source_uri]}), receipt)
    proof = tmp_path / "proof.json"
    proof.write_text('{"status":"qualified"}')
    return {"format": "spicy-source-catalog-release-selection", "version": 1, "candidates": [{
        "task": "T04", "family": "sample", "checks_required": ["independent row binding", "anonymous readback"],
        "qualification": {"path": str(proof), "sha256": pin(proof)},
        "tables": [{"dataset": "sample", "path": str(subject), "sha256": pin(subject), "rows": 1}],
        "receipts": {"path": str(receipt), "sha256": pin(receipt), "generation_id": "generation:test"},
    }]}


def test_pins_inputs_without_claiming_release_or_writing(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch)
    before = set(tmp_path.iterdir())
    result = release.prepare(selected, {"families": {}})
    assert result["candidates"][0]["state"] == "inputs-pinned-awaiting-checks"
    assert result["candidates"][0]["checks_required"]
    assert set(tmp_path.iterdir()) == before


def test_changed_candidate_refused(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch)
    Path(selected["candidates"][0]["tables"][0]["path"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="digest differs"):
        release.prepare(selected, {"families": {}})


def test_private_receipt_path_blocks_publication(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch, source_uri="/Users/operator/private/input.xml")
    result = release.prepare(selected, {"families": {}})["candidates"][0]
    assert result["state"] == "blocked"
    assert result["receipts"]["local_path_rows"] == 1
    assert "Public receipts contain local filesystem paths" in result["blockers"]
    assert "/Users/operator" not in json.dumps(result)


def test_live_newer_rows_and_family_siblings_block(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch)
    index = {"families": {"sample": {"tables": {"sample.parquet": {"rows": 2}, "sibling.parquet": {"rows": 3}}}}}
    result = release.prepare(selected, index)["candidates"][0]
    assert result["state"] == "blocked"
    assert any("smaller than live" in reason for reason in result["blockers"])
    assert any("sibling.parquet" in reason for reason in result["blockers"])


def test_receipt_generation_drift_refused(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch)
    selected["candidates"][0]["receipts"]["generation_id"] = "different"
    with pytest.raises(ValueError, match="generation differs"):
        release.prepare(selected, {"families": {}})


def test_visibility_inspects_nested_arrow_source_witnesses(tmp_path):
    path = tmp_path / "nested.parquet"
    pq.write_table(pa.table({"generation_id": ["g"], "sources": [[{"source_uri": "/Users/private/input.xml"}]]}), path)
    assert release.receipt_visibility(path)["local_path_rows"] == 1
    assert release.contains_local_path({"sources": [{"source_uri": r"C:\private\input.xml"}]})
    assert release.contains_local_path({"sources": [{"source_uri": "/tmp/operator/input.xml"}]})
    assert not release.contains_local_path('"The following areas:\\nreal estate appraisals"')
    assert not release.contains_local_path("https://publisher.example/home/archive")


def test_original_receipt_generations_remain_explicit(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch)
    selected["candidates"][0]["receipts"]["generation_id"] = "new-publishing-bundle"
    selected["candidates"][0]["receipts"]["original_generation_ids"] = ["generation:test"]
    result = release.prepare(selected, {"families": {}})["candidates"][0]
    assert result["receipts"]["generation_ids"] == ["generation:test"]


def test_private_subject_content_blocks_public_selection(tmp_path, monkeypatch):
    selected = selection(tmp_path, monkeypatch)
    table = selected["candidates"][0]["tables"][0]
    pq.write_table(pa.table({"id": ["/Volumes/private/input.xml"]}), table["path"])
    table["sha256"] = pin(Path(table["path"]))
    result = release.prepare(selected, {"families": {}})["candidates"][0]
    assert result["tables"][0]["visibility"]["local_path_rows"] == 1
    assert any("public subjects" in reason for reason in result["blockers"])


def test_qualified_literal_preserves_exact_source_text_and_refuses_new_private_data(tmp_path, monkeypatch):
    import hashlib
    selected = selection(tmp_path, monkeypatch, source_uri="file:///H:/publisher/comment.docx")
    source_proof = tmp_path / "literal-proof.json"
    source_proof.write_text('{"source_xml_sha256":"qualified-original","literal_field":"ABSTRACT"}')
    exceptions = tmp_path / "literal-exceptions.json"
    literal = "file:///H:/publisher/comment.docx"
    exceptions.write_text(json.dumps({"format": "spicy-qualified-public-source-literals", "version": 1,
        "source_proof": {"path": str(source_proof), "sha256": pin(source_proof)},
        "entries": [{"kind": "receipt", "dataset": "sample", "field": "source_uri",
                     "sha256": "sha256:" + hashlib.sha256(literal.encode()).hexdigest(), "occurrences": 1}]}))
    selected["candidates"][0]["literal_source_exceptions"] = {"path": str(exceptions), "sha256": pin(exceptions)}
    result = release.prepare(selected, {"families": {}})["candidates"][0]
    assert result["receipts"]["qualified_source_literal_rows"] == 1
    assert result["receipts"]["local_path_rows"] == 0
    path = Path(selected["candidates"][0]["receipts"]["path"])
    pq.write_table(pa.table({"dataset": ["sample"], "generation_id": ["generation:test"], "source_uri": ["file:///Users/operator/new"]}), path)
    selected["candidates"][0]["receipts"]["sha256"] = pin(path)
    with pytest.raises(ValueError, match="literal occurrence count differs"):
        release.prepare(selected, {"families": {}})


def test_literal_receipt_exceptions_are_scoped_to_dataset(tmp_path):
    import hashlib
    literal = "file:///H:/publisher/comment.docx"
    path = tmp_path / "shared.parquet"
    pq.write_table(pa.table({"dataset": ["a", "b"], "generation_id": ["g", "g"], "source_uri": [literal, literal]}), path)
    entry = {"dataset": "a", "field": "source_uri", "sha256": "sha256:" + hashlib.sha256(literal.encode()).hexdigest(),
             "occurrences": 1}
    result = release.receipt_visibility(path, literal_values=[entry])
    assert result["qualified_source_literal_rows"] == 1
    assert result["local_path_rows"] == 1
    result = release.receipt_visibility(path, literal_values=[entry, {**entry, "dataset": "b"}])
    assert result["qualified_source_literal_rows"] == 2
    assert result["local_path_rows"] == 0


def catalog(tmp_path):
    generation = "sha256:" + "a" * 64
    directory = tmp_path / "generations" / ("a" * 64)
    directory.mkdir(parents=True)
    (tmp_path / "manifest.json").write_text(json.dumps({"generation": generation, "collections": [
        {"id": "collection:one", "detailPath": "generations/" + "a" * 64 + "/collection.json"}]}))
    detail = {"generation": generation, "collection": {"id": "collection:one"}, "files": {"pages": [
        {"path": "generations/" + "a" * 64 + "/page.json", "rows": 1}], "total": 1}}
    (directory / "collection.json").write_text(json.dumps(detail))
    (directory / "page.json").write_text(json.dumps({"generation": generation, "collectionId": "collection:one", "files": [{}]}))
    return directory


def test_catalog_closure_rejects_individually_valid_stale_page(tmp_path):
    directory = catalog(tmp_path)
    assert release.catalog_closure(tmp_path)["pages"] == 1
    page = json.loads((directory / "page.json").read_text())
    page["generation"] = "sha256:" + "b" * 64
    (directory / "page.json").write_text(json.dumps(page))
    with pytest.raises(ValueError, match="generation differs"):
        release.catalog_closure(tmp_path)


def test_catalog_closure_requires_all_referenced_pages(tmp_path):
    directory = catalog(tmp_path)
    (directory / "page.json").unlink()
    with pytest.raises(FileNotFoundError):
        release.catalog_closure(tmp_path)
