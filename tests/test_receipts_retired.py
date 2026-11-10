"""A table whose receipts are retired, through every reader that took "installed policy" to mean "has receipts".

The marker (``subject_catalog.RECEIPTS_RETIRED``) sits beside the table's descriptor in its installed policy file, so
retiring moves no policy version and no ledger entry. These tests mark (or unmark) a table by patching the one
function every reader asks (``subject_catalog.retired_receipt_datasets``).
"""

import copy
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import subject_catalog
from spicy_regs.policy_migrations import descriptor_digest, ledger, unreadable_shipped_policies

REGISTER = Path(__file__).parent / "fixtures/federal_register_switch/expected/night-1.parquet"


@pytest.fixture
def retire(monkeypatch):
    """Mark tables retired for one test, as their installed files would."""
    def mark(*names):
        monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset(names))
    return mark


def test_the_installed_files_retire_exactly_the_switched_tables():
    """Comments and their index left receipts first (ss-juz8): the mirror publishes the catalog table alone."""
    subject_catalog.retired_receipt_datasets.cache_clear()
    assert subject_catalog.retired_receipt_datasets() == frozenset({"comments", "comments_index"})


def test_the_marker_is_a_tables_and_never_part_of_its_policy():
    declared = {**subject_catalog.descriptors()["federal_register"], subject_catalog.RECEIPTS_RETIRED: True}
    assert subject_catalog.is_receipts_retired(declared)
    # Stripped before digesting: the policy, its version and its ledger entry are what they were.
    assert subject_catalog.descriptor_of(declared) == subject_catalog.descriptors()["federal_register"]
    assert descriptor_digest(subject_catalog.policies()["federal_register"]) == ledger()["federal_register"]["installed"]
    shared_log = subject_catalog.descriptors()["congress_acquisition"]
    for refused in ({**declared, subject_catalog.RECEIPTS_RETIRED: "yes"},
                    {**shared_log, subject_catalog.RECEIPTS_RETIRED: True},
                    {**declared, subject_catalog.SHARED_LOG: True}):
        with pytest.raises(ValueError):
            subject_catalog.descriptor_of(refused)


def test_admission_needs_no_receipts_for_a_retired_table_and_refuses_them(retire):
    from spicy_regs.etl_policy_registry import require_registered_receipts

    tables = {"federal_register.parquet": {}}
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        require_registered_receipts(tables, None)
    retire("federal_register")
    require_registered_receipts(tables, None)
    stated = {"policies": [subject_catalog.descriptors()["federal_register"]]}
    with pytest.raises(ValueError, match=r"Receipts are retired for \['federal_register'\]"):
        require_registered_receipts(tables, stated)
    # Every other registered table still needs its receipts.
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        require_registered_receipts({**tables, "fr_docket_links.parquet": {}}, None)


def test_the_ledger_guard_leaves_a_retired_tables_entry_as_the_record_of_its_receipts(retire):
    from spicy_regs.etl_policy_registry import installed_policies

    installed = installed_policies()
    current = installed["federal_register"]
    installed["federal_register"] = replace(current, subject_schema=current.subject_schema.append(
        pa.field("added_later", pa.string())), policy_version="regulations-native-v9")
    migrations = copy.deepcopy(dict(ledger()))
    assert [p.split(":")[0] for p in unreadable_shipped_policies(installed, migrations)] == ["federal_register"]
    retire("federal_register")
    assert unreadable_shipped_policies(installed, migrations) == []


def test_the_generator_keeps_the_marker_its_installed_file_carries(retire):
    """``scripts/update_etl_policy_declarations.py`` regenerates every declaration; the marker survives it."""
    from scripts.update_etl_policy_declarations import declarations

    retire("federal_register")
    declared = declarations(frozenset({"federal_register", "fr_docket_links"}))
    assert declared["federal_register"][subject_catalog.RECEIPTS_RETIRED] is True
    assert subject_catalog.RECEIPTS_RETIRED not in declared["fr_docket_links"]
    assert subject_catalog.descriptor_of(declared["federal_register"]) == subject_catalog.descriptors()["federal_register"]


def test_receipt_lookup_says_the_fields_are_columns_and_reads_nothing(retire):
    from spicy_regs import receipt_lookup

    assert receipt_lookup.carried_fields("federal_register") is not None
    retire("federal_register")
    assert receipt_lookup.carried_fields("federal_register") is None
    described = receipt_lookup.describe("federal_register", {"columns": []}, available=True)
    assert described is not None
    described, meanings = described
    assert described["available"] is False and "table's own columns" in described["retired"] and meanings == {}
    with pytest.raises(ValueError, match="no longer publishes per-row ETL receipts.*There is no receipt to read"):
        receipt_lookup.read_fields(None, table="federal_register", keys=[{}], fields=["html_url"],
                                   index={"families": {}}, local=None, base_url="", entry={}, plain=str)
    assert "federal_register" not in receipt_lookup.tables_with_receipts({"families": {}}, None, "")


def test_no_receipt_field_declaration_for_a_retired_table(retire):
    from spicy_regs import receipt_field_declarations

    tables = receipt_field_declarations.record()["tables"]
    marked = {marker.table for marker in receipt_field_declarations.read_markers()}
    plain = next(name for name in sorted(tables) if name not in marked)
    retire(plain)
    assert plain not in receipt_field_declarations.record()["tables"]
    # A builder's read marker on a retired table governs nothing: the switch removes it with the receipts.
    retire(sorted(marked)[0])
    with pytest.raises(ValueError, match="Read markers name tables with no receipts to read"):
        receipt_field_declarations.record()


def test_the_dictionary_lists_no_receipts_for_a_retired_table_and_checks_it_as_native(retire):
    from spicy_regs import data_dictionary

    assert "federal_register" in {d["dataset"] for d in data_dictionary.receipt_metadata()["datasets"]}
    retire("federal_register")
    assert "federal_register" not in {d["dataset"] for d in data_dictionary.receipt_metadata()["datasets"]}
    # A family entry with no receipt member is a legacy (pre-receipts) table, except a retired one: that one is the
    # native table by design and is checked against the native declaration.
    index = {"families": {"federal-register": {"tables": {"federal_register.parquet": {}}}}}
    _, schemas = data_dictionary.selected_dictionary_definitions(index, ["federal_register"],
                                                                 data_dictionary.DEFAULT_DESCRIPTIONS)
    assert schemas["federal_register"] == data_dictionary.expected_schemas()["federal_register"]


def test_the_receipt_adapter_reads_a_retired_table_as_published(tmp_path, retire):
    import duckdb

    from spicy_regs.fec_receipt_adapter import ReceiptAdapter, published_without_receipts

    key = "federal_register.parquet"
    (tmp_path / key).write_bytes(REGISTER.read_bytes())  # a local mirror holds a member by its key
    digest = "sha256:" + hashlib.sha256(REGISTER.read_bytes()).hexdigest()
    rows = pq.ParquetFile(REGISTER).metadata.num_rows
    index = {"families": {"federal-register": {"prefix": "generations/federal-register/" + "a" * 64, "tables": {
        key: {"sha256": digest, "byteSize": REGISTER.stat().st_size, "rows": rows}}}}}
    adapter = ReceiptAdapter(duckdb.connect(), index, "", local_directory=tmp_path)
    assert not published_without_receipts(index, "federal_register")
    with pytest.raises(ValueError, match="require selected native receipts"):
        list(adapter.selected_rows("federal_register"))
    retire("federal_register")
    assert published_without_receipts(index, "federal_register")
    assert list(adapter.selected_rows("federal_register")) == pq.read_table(REGISTER).to_pylist()


def test_readers_of_schema_and_identity_keep_a_retired_table_as_it_was(retire):
    """explorer_metadata, relationship_views, MCP schemas and the CLI read the declaration's schema and identity."""
    before = json.dumps(subject_catalog.descriptors()["federal_register"], sort_keys=True)
    retire("federal_register")
    assert json.dumps(subject_catalog.descriptors()["federal_register"], sort_keys=True) == before
    assert not subject_catalog.descriptors()["federal_register"]["receipt_only"]


def test_a_retired_parent_is_selected_as_its_pinned_public_table(tmp_path, monkeypatch, retire):
    """``SelectedInputs.select`` (remote): no receipt owner and the marker set give the table alone; unmarked refuses."""
    from spicy_regs.selected_generations import SelectedInputs
    from spicy_regs.sources import publication

    key = "federal_register.parquet"
    digest = "sha256:" + hashlib.sha256(REGISTER.read_bytes()).hexdigest()
    index = {"families": {"federal-register": {"prefix": "generations/federal-register/" + "a" * 64, "tables": {
        key: {"sha256": digest, "byteSize": REGISTER.stat().st_size, "rows": pq.read_metadata(REGISTER).num_rows}}}}}

    def fetch(base, member, target, path):
        target.write_bytes(REGISTER.read_bytes())
        return True

    monkeypatch.setattr(publication, "fetch_member", fetch)
    with pytest.raises(ValueError, match="requires one ETL receipt generation"):
        SelectedInputs(tmp_path / "root", tmp_path / "unmarked", index=index, public_url="https://x.invalid").select(
            "federal_register")
    retire("federal_register")
    selected = SelectedInputs(tmp_path / "root", tmp_path / "marked", index=index,
                              public_url="https://x.invalid").select("federal_register")
    assert selected is not None and selected.receipts is None and selected.generation_id is None
    [subject] = selected.subjects
    assert subject.read_bytes() == REGISTER.read_bytes()


def test_a_receipt_route_consumer_reads_a_retired_parent_and_builds_the_same_table(tmp_path, monkeypatch, retire):
    """Consumers and parents switch in either order: the docket links, still on receipts and declaring no published
    input, read a retired Register table through ``SelectedPriors`` (its regulations reader) and publish the links
    the receipt route published from the Register's receipts. Nothing of the Register's receipts exists to read."""
    from spicy_regs.pipelines.rollups.fr_docket_links import FrDocketLinksRollup
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.receipt_switch import receipt_files, table_differences

    fixture = REGISTER.parents[1]
    root = tmp_path / "output"
    root.mkdir()
    (root / "federal_register.parquet").write_bytes(REGISTER.read_bytes())  # the plain rollup's local table
    retire("federal_register")
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(FrDocketLinksRollup, "published_inputs", ())
    prior = SelectedPriors(tmp_path / "probe", root=root, public_url="")
    restored = prior.get("federal_register")
    assert prior.selections["federal_register"][1:] == (None, None)
    assert "docket_ids_json" in pq.read_schema(restored).names  # the builder's text shape, not the native lists
    FrDocketLinksRollup(output_dir=root).run()
    [generation] = list((root / "generations").iterdir())
    assert table_differences(generation / "fr_docket_links.parquet", fixture / "expected/links-night-1.parquet",
                             key=("docket_id", "document_number", "publication_date")) == []
    parents = json.loads((generation / "artifact.json").read_text())["spec"]["parents"]
    assert parents["federal_register/subjects/0.parquet"]["sha256"] == "sha256:" + hashlib.sha256(
        REGISTER.read_bytes()).hexdigest()
    assert not any(name.startswith("federal_register/etl_receipts") for name in parents)
    assert [path.name for path in receipt_files(root) if "federal_register" in str(path)] == []


def test_a_retired_parent_without_a_reader_for_its_mapper_refuses_by_name(tmp_path, retire):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    root = tmp_path / "output"
    root.mkdir()
    (root / "bill_cosponsors.parquet").write_bytes(REGISTER.read_bytes())
    retire("bill_cosponsors")
    with pytest.raises(ValueError, match="bill_cosponsors: its receipts are retired and its mapper declares no reader"):
        SelectedPriors(tmp_path / "probe", root=root, public_url="").get("bill_cosponsors")
