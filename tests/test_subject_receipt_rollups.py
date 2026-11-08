"""Scheduled producer integration keeps selected processing priors out of subjects."""

import hashlib
import shutil

import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, SubjectReceiptRollup
from spicy_regs.sources import publication
from tests.test_congress_receipts import shaped
from tests.test_legislative_receipts import retained, row


class MixedRollup(SubjectReceiptRollup):
    name = "mixed-test"
    outputs = ("members.parquet", "laws.parquet", "bill_family_archives.parquet")

    def build(self, output_dir):
        raise NotImplementedError


def mixed_builder(directory, *, download_prior):
    # A retained empty technical read still has file metadata and must resume.
    archive = directory / "bill_family_archives.parquet"
    if not download_prior(archive.name, archive):
        shaped(archive, [{"congress": "119", "bill_type": "hr", "size": "50"}], {b"completed": b"yes"})
    member = directory / "members.parquet"
    if not download_prior(member.name, member):
        shaped(member, [{"bioguide_id": "X", "fec_ids_json": '[ "A", "A" ]', "observed_at": "then"}])
    law = directory / "laws.parquet"
    if not download_prior(law.name, law):
        retained(
            directory,
            "laws",
            [
                row(
                    "laws",
                    law_id="119-public-1",
                    congress="119",
                    law_type="public",
                    number="1",
                    uslm_outcome="request_failed",
                )
            ],
        )
    return member, law, archive


def select_bundle(monkeypatch, directory, pipeline):
    generation = pipeline.receipt_generation_id
    receipt = directory / "etl_receipts.parquet"
    keys = list(pipeline.outputs)
    index = publication.empty_index()

    def entry(path):
        return {
            "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
            "byteSize": path.stat().st_size,
            "rows": pq.read_metadata(path).num_rows,
        }

    tables = {}
    for key in keys:
        path = directory / key
        if path.exists():
            tables[key] = entry(path)
        else:
            root = directory / key.removesuffix(".parquet")
            members = [{"key": str(p.relative_to(directory)), **entry(p)} for p in sorted(root.rglob("*.parquet"))]
            tables[key] = {"members": members, "rows": sum(m["rows"] for m in members)}
    index["families"][pipeline.name] = {
        "prefix": "generations/mixed/" + "a" * 64,
        "tables": tables,
        "etlReceipts": {
            "key": receipt.name,
            **entry(receipt),
            "generationId": generation,
            "datasets": [p.dataset for p in pipeline.receipt_policies],
        },
    }
    monkeypatch.setenv("R2_PUBLIC_URL", "https://selected.invalid")
    monkeypatch.setattr(publication, "current_index", lambda _: index)

    def fetch(base, member, target, label):
        source = directory / member.key
        assert member.sha256 == entry(source)["sha256"]
        shutil.copyfile(source, target)
        return True

    monkeypatch.setattr(publication, "fetch_member", fetch)
    return index


def test_mixed_family_restores_both_owners_and_receipt_only_archive(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = MixedRollup()
    outputs = first.build_receipts(first_dir, mixed_builder)
    assert {p.name for p in outputs} == {"members.parquet", "laws.parquet"}
    assert "bill_family_archives.parquet" in first.receipt_only_tables
    assert "observed_at" not in pq.read_schema(first_dir / "members.parquet").names
    assert pq.read_table(first_dir / "members.parquet")["fec_ids"].to_pylist() == [["A", "A"]]
    # A law's read outcome is published with the law, and restored below from the same row's receipt.
    assert pq.read_table(first_dir / "laws.parquet")["uslm_outcome"].to_pylist() == ["request_failed"]
    select_bundle(monkeypatch, first_dir, first)
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    second = MixedRollup()
    again = second.build_receipts(second_dir, mixed_builder)
    assert {p.name for p in again} == {p.name for p in outputs}
    for path in outputs:
        assert pq.read_table(path).equals(pq.read_table(second_dir / path.name))
    restored = next(second_dir.glob(".builds/*/source-output/bill_family_archives.parquet"))
    assert pq.read_schema(restored).metadata[b"completed"] == b"yes"
    assert pq.read_table(next(second_dir.glob(".builds/*/source-output/laws.parquet")))["uslm_outcome"].to_pylist() == [
        "request_failed"
    ]


def test_missing_receipts_refuse_before_processing_read(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    directory = tmp_path / "first"
    directory.mkdir()
    first = MixedRollup()
    first.build_receipts(directory, mixed_builder)
    index = select_bundle(monkeypatch, directory, first)
    del index["families"][first.name]["etlReceipts"]
    with pytest.raises(ValueError, match="native input requires"):
        SelectedPriors(tmp_path / "bad").get("members")


def test_bad_native_value_keeps_refusal_and_cannot_admit_complete_family(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    class Members(SubjectReceiptRollup):
        name = "members-test"
        output = "members.parquet"

        def build(self, output_dir):
            raise NotImplementedError

    def bad(directory, *, download_prior):
        return shaped(directory / "members.parquet", [{"bioguide_id": "X", "fec_ids_json": "bad"}])

    with pytest.raises(ValueError, match="conversion refused"):
        Members().build_receipts(tmp_path, bad)
    assert not (tmp_path / "etl_receipts.parquet").exists()
    [receipt] = (tmp_path / ".builds").glob("*/candidate/etl_receipts.parquet")
    assert "refused" in pq.read_table(receipt)["outcome"].to_pylist()


def test_held_citations_carries_exact_sibling_receipts_and_zero_result_checkpoint(tmp_path, monkeypatch):
    import json
    import pyarrow as pa
    from spicy_regs.legislative_receipts import write_legislative_outputs, restore_prior
    from spicy_regs.pipelines.rollups.held_citations import HeldCitationsRollup
    from spicy_regs.etl_receipts import validate_receipt_bundle
    from spicy_regs.transforms.held_citations import NAMESPACE
    from spicy_regs.transforms.read_checkpoints import read_checkpoints

    raws = [
        retained(tmp_path / "raw", name, [row(name, package_id="BUDGET-2027-BUD")] if name == "budget_volumes" else [])
        for name in ("house_activity_reports", "budget_volumes", "bill_committee_actions", "document_citations")
    ]
    bundle = tmp_path / "bundle"
    write_legislative_outputs(raws, bundle, generation_id="before")
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    manifest = tmp_path / "selection.json"
    manifest.write_text(
        json.dumps(
            {
                "selections": [{"kind": "comment_inline", "keys": ["C-1"]}],
                "input_generations": {"comments": "sha256:" + "a" * 64},
            }
        )
    )
    source = tmp_path / "comments.parquet"
    pq.write_table(pa.Table.from_pylist([{"comment_id": "C-1", "comment": "No legal citations here."}]), source)
    pipeline = HeldCitationsRollup(selection=manifest)
    pipeline.source_members = {"comments": str(source)}
    pipeline.input_pins = {"comments": {"artifactDigest": "sha256:" + "a" * 64}}
    monkeypatch.setattr("spicy_regs.pipelines.rollups.held_citations.load_public_http", lambda *_: None)
    output = tmp_path / "output"
    output.mkdir()
    from spicy_regs.selected_generations import remember_selection, SelectedDataset
    from spicy_regs.navigation_read_outcomes import POLICIES as OUTCOME_POLICIES

    remember_selection(
        output,
        [
            SelectedDataset(
                p.dataset,
                () if p.receipt_only else (bundle / (p.dataset + ".parquet"),),
                bundle / "etl_receipts.parquet",
                "before",
            )
            for p in pipeline.receipt_policies
            if p.dataset not in OUTCOME_POLICIES
        ],
    )
    paths = pipeline.build(output)
    assert {p.name for p in paths} == set(pipeline.outputs)
    validate_receipt_bundle(
        {p.dataset: [] for p in pipeline.receipt_policies} | {p.stem: [p] for p in paths},
        [output / "etl_receipts.parquet"],
        pipeline.receipt_policies,
        generation_id=pipeline.receipt_generation_id,
    )
    before = pq.read_table(bundle / "etl_receipts.parquet").to_pylist()
    after = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    for old in before:
        if old["dataset"] == "budget_volumes":
            [new] = [r for r in after if r["dataset"] == old["dataset"] and r["record_id"] == old["record_id"]]
            assert new["witnesses"][: len(old["witnesses"])] == old["witnesses"]
    restored = restore_prior(next((output / ".builds").glob("*/legislative-bundle")), tmp_path / "restored")
    checkpoint = read_checkpoints(restored["document_citations"], NAMESPACE)
    assert len(checkpoint) == 1 and checkpoint[0]["findings"] == 0


def test_scheduled_fec_catalog_emits_public_catalog_and_receipts(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.fec_source_catalog import FecSourceCatalogRollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    rollup = FecSourceCatalogRollup()
    assert rollup.build(tmp_path) == tmp_path / "fec_source_catalog.parquet"
    subjects = pq.read_table(tmp_path / "fec_source_catalog.parquet")
    assert subjects.num_rows > 0
    receipts = pq.read_table(tmp_path / "etl_receipts.parquet")
    assert receipts.num_rows > 0
    assert set(receipts["dataset"].to_pylist()) == {"fec_source_catalog"}
    assert set(receipts["outcome"].to_pylist()) == {"accepted"}
    assert receipts.num_rows == subjects.num_rows


def test_scheduled_fec_committee_cold_increment_refuses(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.fec_committees import FecCommitteesRollup

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("FEC_COMMITTEES_FULL_WALK", raising=False)
    with pytest.raises(ValueError, match="first build requires full_walk"):
        FecCommitteesRollup().build(tmp_path)


def test_actual_bill_family_resumes_both_owners_without_rereading_bodies(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_bill_family import StubBulkAcquirer, StubBodyAcquirer

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "R2_PUBLIC_URL"):
        monkeypatch.delenv(key, raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = BillFamilyRollup()
    first.build_receipts(
        first_dir, build_bill_family, bulk_acquirer=StubBulkAcquirer(), body_acquirer=StubBodyAcquirer()
    )
    select_bundle(monkeypatch, first_dir, first)
    bulk, body = StubBulkAcquirer(), StubBodyAcquirer()
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    BillFamilyRollup().build_receipts(second_dir, build_bill_family, bulk_acquirer=bulk, body_acquirer=body)
    assert bulk.zip_downloads == []
    assert body.requested == []
    assert list((second_dir / "bill_sections").rglob("*.parquet"))
    assert not (second_dir / "bill_family_archives.parquet").exists()
    # With nothing re-read, the second run's rows are the prior pair's: the columns a reader judges a row by are
    # in both tables, and the fields left in the receipt are in neither.
    returned = {
        "congress_bills": {
            "stage_rule": ["other_chamber"],
            "stage_source_text": ["Received in the Senate."],
            "signed_date_rule": ["no_public_law"],
        },
        "bill_actions": {"source_system_name": ["Senate", "House floor actions"]},
    }
    for table, columns in returned.items():
        for directory in (first_dir, second_dir):
            published = pq.read_table(directory / (table + ".parquet"))
            assert {name: published[name].to_pylist() for name in columns} == columns
            assert not {"stage_matcher", "source_system_code", "url"} & set(published.schema.names)


def test_local_mixed_rollup_can_build_twice_in_one_output_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    pipeline = MixedRollup()
    first = pipeline.build_receipts(tmp_path, mixed_builder)
    before = {path.name: pq.read_table(path).to_pylist() for path in first}
    second = pipeline.build_receipts(tmp_path, mixed_builder)
    assert {path.name: pq.read_table(path).to_pylist() for path in second} == before
    assert len(list((tmp_path / ".builds").iterdir())) == 2


def test_selected_legacy_generation_refuses_without_conversion(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import pyarrow as pa

    directory = tmp_path / "legacy"
    directory.mkdir()
    outputs = mixed_builder(directory, download_prior=lambda *_: False)
    pq.write_table(pa.table({"unused": []}), directory / "etl_receipts.parquet")
    pipeline = SimpleNamespace(
        name="legacy-mixed",
        receipt_generation_id="unused",
        outputs=tuple(path.name for path in outputs),
        receipt_policies=MixedRollup.receipt_policies,
    )
    index = select_bundle(monkeypatch, directory, pipeline)
    del index["families"][pipeline.name]["etlReceipts"]
    reader = SelectedPriors(tmp_path / "selected")
    for dataset in ("members", "laws", "bill_family_archives"):
        with pytest.raises(ValueError, match="native input requires"):
            reader.get(dataset)
    assert not list(tmp_path.rglob("migrated"))


def test_remote_selection_wins_over_stale_local_family_and_local_snapshot_is_fixed(tmp_path, monkeypatch):
    from spicy_regs.selected_generations import SelectedInputs

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    local = tmp_path / "local"
    local.mkdir()
    MixedRollup().build_receipts(local, mixed_builder)
    captured = SelectedInputs(local, tmp_path / "captured", public_url="")
    old = captured.select("members")

    remote = tmp_path / "remote"
    remote.mkdir()
    pipeline = MixedRollup()

    def newer(directory, *, download_prior):
        paths = mixed_builder(directory, download_prior=download_prior)
        shaped(directory / "members.parquet", [{"bioguide_id": "X", "name_first": "Remote update"}])
        return paths

    pipeline.build_receipts(remote, newer)
    index = select_bundle(monkeypatch, remote, pipeline)
    selected = SelectedPriors(tmp_path / "selected", root=local, index=index)
    assert pq.read_table(selected.get("members"))["name_first"].to_pylist() == ["Remote update"]
    # Explicit local mode sees the pinned local generation, despite configured remote access.
    local_only = SelectedInputs(local, tmp_path / "local-selected", public_url="")
    assert local_only.select("members") == old
    # Advancing a pointer cannot retroactively change an already captured input.
    from spicy_regs.selected_generations import remember_selection

    remember_selection(local, [selected.selected.select("members")])
    assert captured.select("members") == old


def test_mutated_local_member_cannot_establish_a_prior(tmp_path, monkeypatch):
    from spicy_regs.selected_generations import SelectedInputs

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    MixedRollup().build_receipts(tmp_path, mixed_builder)
    selection = SelectedInputs(tmp_path, tmp_path / "one", public_url="").select("members")
    selection.subjects[0].write_bytes(b"changed")
    with pytest.raises(ValueError, match="Selected local native member changed"):
        SelectedPriors(tmp_path / "two", root=tmp_path).get("members")


def test_run_uses_pinned_native_required_input_without_convenience_copy(tmp_path, monkeypatch):
    import json
    from spicy_regs.generations import verify_generation

    class ReadsMembers(SubjectReceiptRollup):
        name = "members-read-test"
        inputs = ("members.parquet",)
        output = "committees.parquet"

        def build(self, output_dir):
            def producer(work):
                path = work / "members.parquet"
                assert pq.read_table(path)["bioguide_id"].to_pylist() == ["X"]
                return shaped(work / "committees.parquet", [{"system_code": "SSJU"}])

            return self.build_receipts(output_dir, producer)

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    MixedRollup().build_receipts(tmp_path, mixed_builder)
    pointer = json.loads((tmp_path / ".native-state/selection.json").read_text())["members"]
    old_receipt_digest = pointer["receipts"]["sha256"]
    (tmp_path / "members.parquet").unlink()
    monkeypatch.setattr("spicy_regs.sources.r2.download", lambda *_: pytest.fail("convenience download"))
    ReadsMembers(output_dir=tmp_path).run()
    [directory] = (tmp_path / "generations").iterdir()
    artifact = verify_generation(directory)
    assert artifact.root["spec"]["parents"]["members/etl_receipts.parquet"]["sha256"] == old_receipt_digest


def test_refused_refresh_preserves_visible_and_selected_native_rows(tmp_path, monkeypatch):
    from spicy_regs.local_data import local_selection

    class Members(SubjectReceiptRollup):
        name = "members-refresh-test"
        output = "members.parquet"

        def build(self, output_dir):
            raise NotImplementedError

    def good(work):
        return shaped(work / "members.parquet", [{"bioguide_id": "X", "fec_ids_json": '["A"]'}])

    def bad(work):
        return shaped(work / "members.parquet", [{"bioguide_id": "X", "fec_ids_json": "broken"}])

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    Members().build_receipts(tmp_path, good)
    visible = (tmp_path / "members.parquet").read_bytes()
    receipt = (tmp_path / "etl_receipts.parquet").read_bytes()
    pointer = (tmp_path / ".native-state/selection.json").read_bytes()
    before = local_selection(tmp_path)
    before_path = before.files["members"][0]
    assert pq.read_table(before_path)["bioguide_id"].to_pylist() == ["X"]
    with pytest.raises(ValueError, match="conversion refused"):
        Members().build_receipts(tmp_path, bad)
    assert (tmp_path / "members.parquet").read_bytes() == visible
    assert (tmp_path / "etl_receipts.parquet").read_bytes() == receipt
    assert (tmp_path / ".native-state/selection.json").read_bytes() == pointer
    selected = local_selection(tmp_path)
    assert pq.read_table(selected.files["members"][0])["bioguide_id"].to_pylist() == ["X"]
    attempts = [
        r for p in (tmp_path / ".builds").glob("*/candidate/etl_receipts.parquet") for r in pq.read_table(p).to_pylist()
    ]
    assert any(r["outcome"] == "refused" for r in attempts)


@pytest.mark.parametrize("identities", [(), ("C00000001", "C00000002")])
def test_fec_committee_multipart_prior_reads_all_members_without_mutating_selection(tmp_path, identities):
    from spicy_regs.etl_receipts import combine_receipts
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from spicy_regs.transforms.build_fec_committees import COLUMNS
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    dataset = "fec_committees"
    originals = [
        dict.fromkeys(COLUMNS) | {"committee_id": identity, "cycles_json": "[ 2024, 2024 ]"} for identity in identities
    ]
    parts = []
    receipts = []
    for ordinal, original in enumerate(originals or [None]):
        directory = tmp_path / "selected" / str(ordinal)
        with IdentityReceiptWriter(directory, generation_id="selected", tables=[dataset]) as writer:
            if original is not None:
                writer.emit(dataset, original, input_witness=WITNESS)
        if original is not None:
            parts.append(directory / (dataset + ".parquet"))
        receipts.append(directory / "etl_receipts.parquet")
    receipt_dir = tmp_path / "selected" / "receipts"
    receipt_dir.mkdir()
    combined = combine_receipts(receipts, receipt_dir / "etl_receipts.parquet")
    root = tmp_path / "output"
    remember_selection(root, [SelectedDataset(dataset, tuple(parts), combined, "selected")])
    before = {
        p.relative_to(tmp_path / "selected"): p.read_bytes() for p in (tmp_path / "selected").rglob("*") if p.is_file()
    }

    restored = SelectedPriors(tmp_path / "private", root=root, public_url="").get(dataset)

    assert pq.read_table(restored).to_pylist() == originals
    after = {
        p.relative_to(tmp_path / "selected"): p.read_bytes() for p in (tmp_path / "selected").rglob("*") if p.is_file()
    }
    assert after == before


def test_selected_input_publication_and_dynamic_parent_use_captured_index(tmp_path, monkeypatch):
    from spicy_regs.sources import r2

    source = tmp_path / 'source'
    source.mkdir()
    writer = MixedRollup()
    writer.build_receipts(source, mixed_builder)
    captured = select_bundle(monkeypatch, source, writer)
    captured['families'][writer.name]['artifactDigest'] = 'sha256:' + 'a' * 64
    selected = SelectedPriors(tmp_path / 'reader', index=captured)
    expected = publication.table_pin(captured, 'members.parquet')
    monkeypatch.setattr(publication, 'current_index', lambda *_: pytest.fail('fresh publication pointer read'))
    with r2.recorded_reads() as reads:
        binding = selected.input_provenance('members')
    assert binding['publication'] == expected
    assert binding['publicationDisposition'] == 'captured-publication'
    assert reads['members.parquet'] == expected
    assert reads['members/etl_receipts.parquet']['sha256'] == binding['receipts']['sha256']
    assert reads['members.parquet']['sha256'] != binding['processing']['sha256']


def test_selected_input_local_witnesses_and_missing_disposition(tmp_path, monkeypatch):
    from spicy_regs.sources import r2

    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    MixedRollup().build_receipts(tmp_path, mixed_builder)
    selected = SelectedPriors(tmp_path / 'reader', root=tmp_path, public_url='')
    with r2.recorded_reads() as reads:
        binding = selected.input_provenance('members')
        assert selected.input_provenance('committees') is None
    assert binding['publication'] is None
    assert binding['publicationDisposition'] == 'local-selection'
    assert reads == {
        'members/subjects/0.parquet': binding['subjects'][0],
        'members/etl_receipts.parquet': binding['receipts'],
    }
    assert all('family' not in pin and 'artifactDigest' not in pin for pin in reads.values())


def test_selected_input_unmapped_remote_subject_is_explicit(tmp_path, monkeypatch):
    from spicy_regs.sources import r2

    source = tmp_path / 'source'
    source.mkdir()
    writer = MixedRollup()
    writer.build_receipts(source, mixed_builder)
    captured = select_bundle(monkeypatch, source, writer)
    captured['families'][writer.name]['artifactDigest'] = 'sha256:' + 'a' * 64
    selected = SelectedPriors(tmp_path / 'reader', index=captured)
    with r2.recorded_reads() as reads:
        binding = selected.input_provenance('bill_family_archives')
    assert binding['publication'] is None
    assert binding['publicationDisposition'] == 'no-published-subject'
    assert 'bill_family_archives.parquet' not in reads
    assert reads['bill_family_archives/etl_receipts.parquet'] == binding['receipts']


def test_dynamic_selected_member_is_a_published_generation_parent(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from spicy_regs.generations import verify_generation

    source = tmp_path / 'source'
    source.mkdir()
    writer = MixedRollup()
    writer.build_receipts(source, mixed_builder)
    captured = select_bundle(monkeypatch, source, writer)
    family = captured['families'][writer.name]
    family['artifactDigest'] = 'sha256:' + 'a' * 64
    family['prefix'] = 'generations/' + writer.name + '/' + 'a' * 64
    family['logicalId'] = 'urn:dynamic-parent-source'
    for key, table in family['tables'].items():
        table['columns'] = [[field.name, str(field.type)] for field in pq.read_schema(source / key)]
    family['etlReceipts']['columns'] = [
        [field.name, str(field.type)] for field in pq.read_schema(source / 'etl_receipts.parquet')
    ]
    expected = publication.table_pin(captured, 'members.parquet')
    monkeypatch.setattr(publication, 'snapshot', lambda _: nullcontext(captured))
    monkeypatch.setattr(publication, 'current_index', lambda *_: pytest.fail('fresh publication pointer read'))

    class DynamicMembers(SubjectReceiptRollup):
        name = 'dynamic-members-read-test'
        inputs = ()
        output = 'committees.parquet'

        def build(self, output_dir):
            def producer(work, *, download_prior, selected_input):
                assert download_prior('members.parquet', work / 'members.parquet')
                assert selected_input('members')['publication'] == expected
                return shaped(work / 'committees.parquet', [{'system_code': 'SSJU'}])

            return self.build_receipts(output_dir, producer)

    output = tmp_path / 'output'
    DynamicMembers(output_dir=output).run()
    [directory] = (output / 'generations').iterdir()
    artifact = verify_generation(directory)
    assert artifact.root['spec']['parents']['members.parquet'] == expected
    assert artifact.root['spec']['parents']['members/etl_receipts.parquet']['sha256'].startswith('sha256:')


def test_split_selected_publication_parent_preserves_descriptor_authority(tmp_path, monkeypatch):
    from spicy_regs.sources import r2

    source = tmp_path / 'source'
    source.mkdir()
    writer = MixedRollup()
    writer.build_receipts(source, mixed_builder)
    members = source / 'members'
    members.mkdir()
    (source / 'members.parquet').rename(members / 'part-0.parquet')
    captured = select_bundle(monkeypatch, source, writer)
    family = captured['families'][writer.name]
    family['artifactDigest'] = 'sha256:' + 'a' * 64
    descriptor = family['tables']['members.parquet']
    descriptor['byteSize'] = sum(member['byteSize'] for member in descriptor['members'])
    descriptor['partitionColumns'] = []
    expected = publication.table_pin(captured, 'members.parquet')
    with r2.recorded_reads() as reads:
        binding = SelectedPriors(tmp_path / 'reader', index=captured).input_provenance('members')
    assert binding['publication'] == reads['members.parquet'] == expected
    assert 'tableDescriptorDigest' in expected and 'sha256' not in expected


def test_historical_refusal_survives_a_nightly_then_success_resolves_it(tmp_path, monkeypatch):
    from spicy_regs.legislative_receipts import write_legislative_outputs

    class Budget(SubjectReceiptRollup):
        name = "budget-refusal-test"
        outputs = ("budget_volumes.parquet",)
        def build(self, output_dir):
            raise NotImplementedError

    bad = row("budget_volumes", package_id="X", associated_bills_json="bad")
    good = row("budget_volumes", package_id="Y")
    source = retained(tmp_path / "initial-source", "budget_volumes", [bad, good])
    write_legislative_outputs([source], tmp_path / "initial", generation_id="initial")
    # Seed an admitted current selection whose source succeeded and whose
    # receipt member still retains the prior unresolved refusal.
    current = Budget()
    selected_source = retained(tmp_path / "selected-source", "budget_volumes", [good])
    write_legislative_outputs([selected_source], tmp_path / "selected", generation_id=current.receipt_generation_id,
                              prior_receipts=[tmp_path / "initial/etl_receipts.parquet"])
    select_bundle(monkeypatch, tmp_path / "selected", current)
    for name, corrected in (("unchanged", False), ("corrected", True)):
        output = tmp_path / name
        output.mkdir()
        def build(directory, *, download_prior):
            previous = directory / "budget_volumes.parquet"
            assert download_prior(previous.name, previous)
            values = pq.read_table(previous).to_pylist()
            if corrected:
                values.append(bad | {"associated_bills_json": "[]"})
            return retained(directory, "budget_volumes", values)
        pipeline = Budget()
        pipeline.build_receipts(output, build)
        outcomes = pq.read_table(output / "etl_receipts.parquet")["outcome"].to_pylist()
        assert ("refused" in outcomes) is not corrected
        select_bundle(monkeypatch, output, pipeline)
