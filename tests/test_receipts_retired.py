"""A table whose receipts are retired, through every reader that took "installed policy" to mean "has receipts".

The marker (``subject_catalog.RECEIPTS_RETIRED``) sits beside the table's descriptor in its installed policy file, so
retiring moves no policy version and no ledger entry. These tests mark (or unmark) a table by patching the one
function every reader asks (``subject_catalog.retired_receipt_datasets``).
Which tables the installed files mark is held in one place: tests/test_receipt_archive_pins.py requires the marked
tables to equal the pinned ones, each pin checked against its generation's own root, so a table marked by accident
(no pin) fails there and a family's switch edits no test here.
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
    """Start from no table retired, then mark the ones a test names, as their installed files would: a family's
    switch changes the installed marks and none of these tests."""
    def mark(*names):
        monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset(names))
    mark()
    return mark


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


def test_the_ledger_guard_checks_a_retired_table_like_any_other(retire):
    """A marked table's policy may still change in the commit that marks it; an unrecorded change still fails."""
    from spicy_regs.etl_policy_registry import installed_policies

    installed = installed_policies()
    current = installed["federal_register"]
    installed["federal_register"] = replace(current, subject_schema=current.subject_schema.append(
        pa.field("added_later", pa.string())), policy_version="regulations-native-v9")
    migrations = copy.deepcopy(dict(ledger()))
    retire("federal_register")
    assert [p.split(":")[0] for p in unreadable_shipped_policies(installed, migrations)] == ["federal_register"]


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


def test_a_receipt_route_reader_gets_a_retired_parent_as_its_mapper_reads_it(tmp_path, monkeypatch, retire):
    """Consumers and parents switch in either order: a reader still on receipts (``SelectedPriors``, which every
    receipt rollup's inputs and soft inputs go through) gets a retired Register table as the regulations builders read
    their input, bound to the table alone. (Until step 3 the docket links were such a consumer, and published from it
    exactly the links the receipt route published; they are a plain rollup now.)"""
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.receipt_switch import table_differences
    from spicy_regs.transforms.regulations_shape import published_as_source

    root = tmp_path / "output"
    root.mkdir()
    (root / "federal_register.parquet").write_bytes(REGISTER.read_bytes())  # the plain rollup's local table
    retire("federal_register")
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    prior = SelectedPriors(tmp_path / "probe", root=root, public_url="")
    restored = prior.get("federal_register")
    assert prior.selections["federal_register"][1:] == (None, None)
    assert "docket_ids_json" in pq.read_schema(restored).names  # the builder's text shape, not the native lists
    expected = published_as_source("federal_register", REGISTER, tmp_path / "expected.parquet")
    assert table_differences(restored, expected, key=("document_number", "publication_date")) == []
    binding = prior.input_provenance("federal_register")
    assert binding["receipts"] is None and binding["subjects"] == [{"sha256": "sha256:" + hashlib.sha256(
        REGISTER.read_bytes()).hexdigest(), "byteSize": REGISTER.stat().st_size}]


def test_a_retired_parent_without_a_reader_for_its_mapper_refuses_by_name(tmp_path, retire):
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    root = tmp_path / "output"
    root.mkdir()
    (root / "bill_cosponsors.parquet").write_bytes(REGISTER.read_bytes())
    retire("bill_cosponsors")
    with pytest.raises(ValueError, match="bill_cosponsors: its receipts are retired and its mapper has no reader"):
        SelectedPriors(tmp_path / "probe", root=root, public_url="").get("bill_cosponsors")


def pin_archive(monkeypatch, tmp_path, archives):
    """Serve ``archives`` as docs/pins/receipt-archive.json to retention's reader, for one test."""
    from spicy_regs import generation_retention

    path = tmp_path / "receipt-archive.json"
    path.write_text(json.dumps({"format": generation_retention.RECEIPT_ARCHIVE_FORMAT, "version": 1,
                                "archives": archives}))
    monkeypatch.setattr(generation_retention, "RECEIPT_ARCHIVE", path)


def _pin(family, digest):
    hexd = digest.removeprefix("sha256:")
    return {"family": family, "artifactDigest": digest, "publishedAt": "2026-10-10T15:45:23Z",
            "receipts": f"generations/{family}/{hexd}/etl_receipts.parquet"}


@pytest.mark.parametrize("stale", [False, True], ids=["live-generation-pinned", "stale-pin"])
def test_the_first_receipts_free_publish_needs_the_live_generation_pinned(tmp_path, monkeypatch, retire, stale):
    """Shaped like comments on 2026-11-10: one family, two tables, both retired together, both pinned to the family's
    live generation (the last with receipts). A pin of any other generation is refused, naming table and both
    digests, and nothing is published."""
    from spicy_regs import etl_policy_registry
    from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, combine_receipts, write_dataset
    from spicy_regs.generations import build_generation
    from spicy_regs.sources.publication import PublicationError, empty_index, publish_generation
    from tests.generation_fakes import Store

    store, schema = Store(), pa.schema([("id", pa.string())])
    policies = [DatasetPolicy(name, schema, ("id",), ("held",)) for name in ("comments", "comments_index")]
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {p.dataset: p for p in policies})
    held = ReceiptContext("build-1", "row", "test", [{"source_id": "s", "body_version": "1"}])
    pairs = [write_dataset([({"id": "1", "held": "x"}, held)], tmp_path / p.dataset, p) for p in policies]
    receipts = combine_receipts([receipt for _, receipt in pairs], tmp_path / "etl_receipts.parquet")
    tables = [subject for subject, _ in pairs if subject is not None]
    keys = [p.dataset + ".parquet" for p in policies]
    build_generation(tmp_path / "with-receipts", family="comments", files=tables, expected_keys=keys,
                     receipt_path=receipts, receipt_policies=policies, receipt_generation_id="build-1")
    index = publish_generation(tmp_path / "with-receipts", client=store, bucket="b", prior_index=empty_index())
    live = index["families"]["comments"]["artifactDigest"]
    pinned = "sha256:" + "ab" * 32 if stale else live
    pin_archive(monkeypatch, tmp_path, {"comments": _pin("comments", live), "comments_index": _pin("comments", pinned)})
    retire("comments", "comments_index")
    build_generation(tmp_path / "receipts-free", family="comments", files=tables, expected_keys=keys)
    if stale:
        with pytest.raises(PublicationError, match=f"comments_index leaves its receipts in comments generation {live}, "
                                                   f"but docs/pins/receipt-archive.json pins comments {pinned}"):
            publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        assert "etlReceipts" in store_index(store)["families"]["comments"]
    else:
        published = publish_generation(tmp_path / "receipts-free", client=store, bucket="b", prior_index=index)
        assert "etlReceipts" not in published["families"]["comments"]


def store_index(store):
    from spicy_regs.sources import publication

    return publication.parse_index(store.objects[publication.INDEX_V2_KEY])


def test_a_retirement_cannot_be_checked_without_the_pin_file(tmp_path, monkeypatch):
    from spicy_regs import generation_retention
    from spicy_regs.sources.publication import PublicationError, _require_receipt_archive_pins

    monkeypatch.setattr(generation_retention, "RECEIPT_ARCHIVE", tmp_path / "missing.json")
    with pytest.raises(PublicationError, match="publish a retirement from a spicy-regs checkout"):
        _require_receipt_archive_pins("comments", "sha256:" + "ab" * 32, {"comments"})


def test_the_mcp_server_serves_no_legacy_meanings_for_a_retired_table(monkeypatch, retire):
    """A family entry without receipts read as a pre-native generation; a retired table's entry has none by design."""
    from spicy_regs import mcp_server

    index = {"families": {"federal-register": {"tables": {"federal_register.parquet": {}}}}}
    monkeypatch.setattr(mcp_server, "_connection_local_selection", lambda cursor: None)
    monkeypatch.setattr(mcp_server, "_connection_index", lambda cursor: index)
    entry = {"legacy_column_descriptions": {"docket_ids_json": "The pre-native spelling."}}
    import duckdb

    with duckdb.connect() as cursor:
        retire()
        assert mcp_server._legacy_column_meanings(cursor, "federal_register", entry) == entry[
            "legacy_column_descriptions"]
        retire("federal_register")
        assert mcp_server._legacy_column_meanings(cursor, "federal_register", entry) == {}


@pytest.mark.parametrize("path", ["restore", "base-family"])
def test_a_regulatory_path_that_still_reads_receipts_refuses_a_retired_table_by_name(tmp_path, monkeypatch, retire,
                                                                                    path):
    """Not the retired route: these paths read a table's receipts, and say so instead of crashing on none."""
    from spicy_regs.pipelines.regulatory_publication import restore_dataset
    from spicy_regs.pipelines.rollups.regulatory_base import DocumentsFamily

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    retire("documents")
    table = pa.table({"document_id": ["D-1"], "title": ["one"]})
    if path == "restore":
        pq.write_table(table, tmp_path / "documents.parquet")
        with pytest.raises(ValueError, match="documents has retired receipts; the regulatory restore still reads them"):
            restore_dataset(tmp_path, "documents", tmp_path / "restored.parquet")
    else:
        (tmp_path / ".selected").mkdir()
        pq.write_table(table, tmp_path / ".selected" / "documents.parquet")
        with pytest.raises(ValueError, match="documents has retired receipts; the base regulatory family still reads"):
            DocumentsFamily(output_dir=tmp_path).build(tmp_path)


def test_a_selection_says_whose_receipts_are_retired():
    from spicy_regs.selected_generations import SelectedDataset

    held = SelectedDataset("documents", (), Path("r.parquet"), "g")
    assert held.required_receipts("a reader") == Path("r.parquet")
    with pytest.raises(ValueError, match="comments has retired receipts; a reader still reads them"):
        SelectedDataset("comments", (), None, None).required_receipts("a reader")
