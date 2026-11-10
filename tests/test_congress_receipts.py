"""Receipt linkage, producer replay and incremental state preservation."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.congress_receipts import (
    CongressBuild,
    CongressInput,
    policy,
    restore_processing_input,
    write_congress_dataset,
)
from spicy_regs.etl_receipts import decode_exact_json, validate_receipt_bundle
from spicy_regs.transforms.build_congress_index import INDEX_SPECS, build_index_table
from tests.test_congress_index import FixtureReader


def shaped(path, rows, metadata=None):
    columns = list(dict.fromkeys(k for row in rows for k in row))
    schema = pa.schema([(c, pa.string()) for c in columns], metadata=metadata)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


@pytest.mark.parametrize("nickname", [None, "Tom", "  Denny  ", "of Carrollton"])
def test_member_nickname_preserves_exact_source_value_through_native_restore(tmp_path, nickname):
    source = shaped(tmp_path / "source.parquet", [
        {"bioguide_id": "X", "name_first": "Thomas", "name_nickname": nickname},
    ])
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset="members", generation_id="g")
    assert pq.read_table(subject)["name_nickname"].to_pylist() == [nickname]
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored.parquet", dataset="members", generation_id="g"
    )
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)


@pytest.mark.parametrize("names", [
    # French Hill (H001072): a first initial, the middle name he goes by, and the House's full name.
    {"name_middle": "French", "name_suffix": None, "name_official_full": "French Hill"},
    {"name_middle": "T.", "name_suffix": "Jr.", "name_official_full": None},
    {"name_middle": None, "name_suffix": "III", "name_official_full": "  Literal  "},
])
def test_member_name_parts_are_classified_and_kept_exactly_through_native_restore(tmp_path, names):
    """spicy-docs 0.85.0's three name columns are subject columns of ``members``, as stated, never refused or dropped."""
    source = shaped(tmp_path / "source.parquet", [{"bioguide_id": "X", "name_first": "J.", **names}])
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset="members", generation_id="g")
    assert pq.read_table(subject).select(list(names)).to_pylist() == [names]
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored.parquet", dataset="members", generation_id="g"
    )
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)


@pytest.mark.parametrize("name", INDEX_SPECS)
def test_real_index_producer_admits_native_subjects_and_resumes_from_receipts(tmp_path, name):
    first = CongressBuild("first")
    reader = FixtureReader()
    subject = build_index_table(
        tmp_path, INDEX_SPECS[name], reader=reader, congresses=[119], max_details=3, receipt_build=first
    )
    first.admit(tmp_path / "generation", family=name.replace("_", "-"), subjects=[subject])
    assert "url" not in pq.read_schema(subject).names
    if name == "committee_meetings":
        assert pa.types.is_list(pq.read_schema(subject).field("witnesses").type)
    assert first.receipt_path is not None
    second = CongressBuild("second", inputs={name: CongressInput(subject, first.receipt_path, "first")})
    next_reader = FixtureReader()
    again = build_index_table(
        tmp_path, INDEX_SPECS[name], reader=next_reader, congresses=[119], max_details=3, receipt_build=second
    )
    before = {tuple(sorted((k, str(v)) for k, v in row.items())) for row in pq.read_table(subject).to_pylist()}
    after = {tuple(sorted((k, str(v)) for k, v in row.items())) for row in pq.read_table(again).to_pylist()}
    assert before == after
    if name == "nominations":
        assert set(reader.details) == set(next_reader.details)  # refused detail routes retry; they are not empty lists
    else:
        assert not set(reader.details) & set(next_reader.details)


def test_missing_receipts_and_changed_subject_cannot_resume(tmp_path):
    source = shaped(tmp_path / "source.parquet", [{"bioguide_id": "X", "fec_ids_json": "[]", "observed_at": "then"}])
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset="members", generation_id="g")
    with pytest.raises((ValueError, OSError)):
        CongressInput(subject, tmp_path / "missing-receipts.parquet", "g").materialize(
            "members", tmp_path / "missing.parquet"
        )
    restored = restore_processing_input(
        subject, receipts, tmp_path / "carried.parquet", dataset="members", generation_id="other"
    )
    assert pq.read_table(restored).equals(pq.read_table(source))
    with pytest.raises(ValueError, match="publisher generation"):
        restore_processing_input(subject, receipts, tmp_path / "empty.parquet", dataset="members", generation_id="")
    table = pq.read_table(subject)
    changed = table.set_column(table.schema.get_field_index("name_first"), "name_first", pa.array(["different"]))
    pq.write_table(changed, tmp_path / "changed.parquet")
    with pytest.raises(ValueError, match="Missing, ambiguous"):
        restore_processing_input(
            tmp_path / "changed.parquet", receipts, tmp_path / "out.parquet", dataset="members", generation_id="g"
        )
    assert not (tmp_path / "out.parquet").exists()


def test_processing_only_archive_and_footer_resume_exactly_without_subject(tmp_path):
    metadata = {
        b"spicy_regs.bill_family.completed_archive_scopes.v4": b'[[119,"hr"]]',
        b"spicy_regs.bill_family.status_refusals.v1": b'{"119-hr-4":{"reader":"p","refusal":"bad"}}',
    }
    source = shaped(
        tmp_path / "source.parquet",
        [{"congress": "119", "bill_type": "hr", "size": "50", "modified_at": "2026-01-01", "observed_at": "then"}],
        metadata,
    )
    subject, receipts = write_congress_dataset(
        source, tmp_path / "bundle", dataset="bill_family_archives", generation_id="g"
    )
    assert subject is None
    assert not (tmp_path / "bundle" / "bill_family_archives.parquet").exists()
    restored = restore_processing_input(
        None, receipts, tmp_path / "restored.parquet", dataset="bill_family_archives", generation_id="g"
    )
    assert pq.read_schema(restored).equals(pq.read_schema(source), check_metadata=True)
    assert pq.read_table(restored).to_pylist() == pq.read_table(source).to_pylist()


def test_invalid_values_have_receipts_without_subjects_and_retry_as_unheld(tmp_path):
    source = shaped(tmp_path / "source.parquet", [{"bill_id": "119-hr-1", "subjects_json": "[malformed"}])
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset="bill_subjects", generation_id="g")
    assert pq.read_table(subject).num_rows == 0
    outcomes = pq.read_table(receipts)["outcome"].to_pylist()
    assert outcomes == ["observed", "refused"]
    assert subject is not None
    validate_receipt_bundle({"bill_subjects": [subject]}, [receipts], [policy("bill_subjects")], generation_id="g")
    restored = restore_processing_input(
        subject, receipts, tmp_path / "retry.parquet", dataset="bill_subjects", generation_id="g"
    )
    assert pq.read_table(restored).num_rows == 0


def test_exact_raw_list_spelling_and_duplicate_witnesses_are_preserved(tmp_path):
    raw = {
        "bioguide_id": "X",
        "fec_ids_json": '[ "A", null, "A" ]',
        "other_names_json": '[{"last":"A","middle":null},{"last":"A"}]',
    }
    source = shaped(tmp_path / "source.parquet", [raw])
    witness = {
        "source_id": "retained-roster",
        "sha256": "a" * 64,
        "locator": None,
        "body_version": None,
        "source_uri": None,
    }
    subject, receipts = write_congress_dataset(
        source, tmp_path / "bundle", dataset="members", generation_id="g", witnesses=[witness, witness]
    )
    assert pq.read_table(subject)["fec_ids"].to_pylist() == [["A", None, "A"]]
    accepted = [r for r in pq.read_table(receipts).to_pylist() if r["outcome"] == "accepted"][0]
    assert accepted["witnesses"][1:] == [witness, witness]
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored.parquet", dataset="members", generation_id="g"
    )
    assert pq.read_table(restored).to_pylist() == [raw]


def test_unresolved_term_assignment_remains_receipt_only(tmp_path):
    raw = {"vote_id": "119-house-1-1", "member_key": "name:A", "term_match": "unresolved_member", "term_index": None}
    source = shaped(tmp_path / "source.parquet", [raw])
    subject, receipts = write_congress_dataset(
        source, tmp_path / "bundle", dataset="member_vote_terms", generation_id="g"
    )
    assert pq.read_table(subject).num_rows == 0
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored.parquet", dataset="member_vote_terms", generation_id="g"
    )
    assert pq.read_table(restored).to_pylist() == [raw]


def test_receipt_only_backfills_resume_capped_walk(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.bill_family import BillFamilyRollup
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_bill_family import _stub_source, _two_bills, _numbers

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "92")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    first_source = _stub_source(_two_bills())
    BillFamilyRollup().build_receipts(tmp_path, build_bill_family, list_source=first_source, max_version_fetches=2)
    assert _numbers(first_source.requested) == [2185]
    second_source = _stub_source(_two_bills())
    paths = BillFamilyRollup().build_receipts(tmp_path, build_bill_family, list_source=second_source)
    assert _numbers(second_source.requested) == [2190]
    bills = next(p for p in paths if p.stem == "congress_bills")
    assert pq.read_table(bills)["bill_id"].to_pylist() == ["92-hr-2185", "92-hr-2190"]


def test_missing_subject_key_is_a_retained_refusal(tmp_path):
    source = shaped(tmp_path / "source.parquet", [{"bioguide_id": None, "name_first": "Unknown"}])
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset="members", generation_id="g")
    assert pq.read_table(subject).num_rows == 0
    assert pq.read_table(receipts)["outcome"].to_pylist() == ["observed", "refused"]


def test_next_generation_names_only_its_predecessor_when_subject_changes(tmp_path):
    first_raw = shaped(tmp_path / "first.parquet", [{"bioguide_id": "X", "name_first": "First"}])
    witness = {
        "source_id": "native-capture",
        "sha256": "b" * 64,
        "locator": "entry:7",
        "body_version": None,
        "source_uri": None,
    }
    subject, receipts = write_congress_dataset(
        first_raw, tmp_path / "first", dataset="members", generation_id="first", witnesses=[witness, witness]
    )
    next_raw = shaped(tmp_path / "next.parquet", [{"bioguide_id": "X", "name_first": "Next"}])
    _, next_receipts = write_congress_dataset(
        next_raw,
        tmp_path / "next",
        dataset="members",
        generation_id="next",
        prior=CongressInput(subject, receipts, "first"),
    )
    before = next(r for r in pq.read_table(receipts).to_pylist() if r["outcome"] == "accepted")
    after = next(r for r in pq.read_table(next_receipts).to_pylist() if r["outcome"] == "accepted")
    assert after["record_id"] == before["record_id"]
    assert after["subject_version"] != before["subject_version"]
    # The carry rule: the changed row's witnesses are its own, and it points to its direct predecessor.
    assert [w["source_id"] for w in after["witnesses"]] == ["shaped-observation:members"]
    reference = decode_exact_json(after["diagnostic_json"])["prior_receipt"]
    assert reference["receipt_id"] == before["receipt_id"] and reference["generation_id"] == "first"
    assert after["generation_id"] == "next"


def test_members_source_journal_is_retained_in_shared_receipts(tmp_path):
    from spicy_regs.congress_receipts import ACQUISITION_POLICY
    from spicy_regs.etl_receipts import read_attempts, select_receipts
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.build_members import build_members
    from tests.test_source_evidence import legislators, roster, journal

    evidence = CaptureEvidence(tmp_path / "capture", "members")
    build = CongressBuild("members")
    with legislators(evidence, roster()) as acquirer:
        subjects = build_members(tmp_path, acquirer=acquirer, evidence=evidence, receipt_build=build)
    assert build.receipt_path is not None
    scoped = select_receipts(build.receipt_path, tmp_path / "capture-receipts.parquet", dataset="congress_acquisition")
    events = list(read_attempts([scoped], ACQUISITION_POLICY, generation_id="members"))
    assert [
        e["processing_fields"]["capture_event"] for e in events if "capture_event" in e["processing_fields"]
    ] == journal(evidence)
    assert all("observed_at" not in pq.read_schema(path).names for path in subjects)
    build.admit(tmp_path / "generation", family="members", subjects=subjects)


def test_failed_builder_retains_scrubbed_journal_and_cannot_admit(tmp_path):
    from spicy_regs.congress_receipts import ACQUISITION_POLICY
    from spicy_regs.etl_receipts import read_attempts
    from spicy_regs.source_evidence import CaptureEvidence
    from tests.test_source_evidence import KEY, journal

    evidence = CaptureEvidence(tmp_path / "capture", "failed")
    evidence.credential = KEY
    error = ValueError("bad page https://source.test/?api_key=" + KEY)

    def failed(output_dir, *, evidence):
        evidence.refusal(error, stage="listing")
        raise error

    build = CongressBuild("failed")
    with pytest.raises(ValueError):
        build.run(failed, tmp_path, evidence=evidence)
    assert build.receipt_path is not None
    attempts = list(read_attempts([build.receipt_path], ACQUISITION_POLICY, generation_id="failed"))
    assert attempts[0]["outcome"] == "error"
    assert [a["processing_fields"]["capture_event"] for a in attempts[1:]] == journal(evidence)
    assert KEY not in str(attempts)
    with pytest.raises(ValueError, match="No completed Congress build"):
        build.admit(tmp_path / "must-not-publish", family="members", subjects=[])


def test_bill_subject_producer_keeps_occurrence_order_and_empty_unknown_distinction(tmp_path):
    from spicy_regs.transforms.enrich_bill_subjects import enrich_bill_subjects
    from tests.test_bill_subjects import _write_bills, _bill, _Folders, _StubFetcher

    source = tmp_path / "bills.parquet"
    _write_bills(
        source,
        [
            _bill("119-hr-1", family=("Health", ["A", None, "", "A"])),
            _bill("119-hr-2", family=("Health", [])),
            _bill("119-hr-3", family=("Health", None)),
        ],
    )
    subject, receipts = write_congress_dataset(
        source, tmp_path / "bills-native", dataset="congress_bills", generation_id="prior"
    )
    build = CongressBuild("subjects", inputs={"congress_bills": CongressInput(subject, receipts, "prior")})
    result = enrich_bill_subjects(tmp_path, read_folder=_Folders(), fetcher=_StubFetcher(), receipt_build=build)
    rows = {r["bill_id"]: r for r in pq.read_table(result).to_pylist()}
    assert rows["119-hr-1"]["subjects"] == ["A", None, "", "A"]
    assert rows["119-hr-1"]["subject_count"] == 4
    assert rows["119-hr-2"]["subjects"] == []
    assert "119-hr-3" not in rows


#: Receipt fields a reader needs to judge a row, returned to their tables (owner decision, 2026-10-05). Per table:
#: the returned columns; one row as its producer shapes it, every value text (the rows published 2026-10-04,
#: abridged); fields of that row which stay in the receipt; and a later read's different answer for one column.
RETURNED = {
    "congress_bills": (
        ("stage_rule", "stage_source_text", "signed_date_rule"),
        {
            "bill_id": "119-hr-1",
            "stage": "law",
            "stage_rule": "became_law_code",
            "stage_matcher": "E40000",
            "stage_action_index": "0",
            "stage_source_text": "Became Public Law No: 119-21.",
            "signed_date": "2025-07-04",
            "signed_date_rule": "public_law_and_became_law_action",
            "signed_date_action_code": "E40000",
            "url": "https://www.congress.gov/bill/119th-congress/house-bill/1",
        },
        ("stage_matcher", "stage_action_index", "signed_date_action_code", "url"),
        {"stage_rule": "law"},
    ),
    "bill_actions": (
        ("source_system_name",),
        {
            "bill_id": "119-hr-1",
            "action_index": "2",
            "action_date": "2025-07-04",
            "action_text": "Signed by President.",
            "action_code": "E30000",
            "source_system_code": "9",
            "source_system_name": "Library of Congress",
            "stage": "law",
            "stage_rule": "law",
            "stage_matcher": "signed by president",
        },
        # stage_rule returned to congress_bills only: an action's own rule stays in its receipt.
        ("source_system_code", "stage_rule", "stage_matcher"),
        {"source_system_name": "House floor actions"},
    ),
    "committee_assignments": (
        ("congress_basis",),
        {
            "congress": "119",
            "congress_basis": "caller",
            "chamber": "senate",
            "system_code": "spag00",
            "committee_code": "SPAG00",
            "bioguide_id": "A000382",
            "lis_id": "S428",
            "file_date": "Saturday, October 3, 2026",
            "observed_at": "2026-10-04T02:30:46.792030Z",
        },
        ("file_date", "observed_at"),
        {"congress_basis": "file"},
    ),
}


@pytest.mark.parametrize("dataset", RETURNED)
def test_returned_columns_are_text_columns_of_the_installed_policy_at_its_first_version(dataset):
    from spicy_regs.etl_policy_registry import installed_policies

    columns, _, kept, _ = RETURNED[dataset]
    declared = policy(dataset)
    for column in columns:
        assert declared.subject_schema.field(column).type == pa.string()
    assert not set(kept) & set(declared.subject_schema.names)
    # These tables had not published natively when the columns returned, so no reader held an earlier schema then.
    # committee_assignments has since gained its directory columns (ss-ehci): /2, with /1 kept in history.
    assert declared.policy_version == (
        "congress-subjects/2" if dataset == "committee_assignments" else "congress-subjects/1"
    )
    # Publication admits a generation against the installed declaration, not this module's.
    assert installed_policies()[dataset].descriptor() == declared.descriptor()


@pytest.mark.parametrize("dataset", RETURNED)
def test_returned_values_are_published_and_the_receipt_still_holds_the_whole_original_row(tmp_path, dataset):
    from spicy_regs.etl_receipts import decode_exact_json

    columns, raw, kept, _ = RETURNED[dataset]
    source = shaped(tmp_path / "source.parquet", [raw])
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset=dataset, generation_id="g")
    assert subject is not None
    published = pq.read_table(subject)
    for column in columns:
        assert published.schema.field(column).type == pa.string()
        assert published[column].to_pylist() == [raw[column]]
    assert not set(kept) & set(published.schema.names)
    [accepted] = [r for r in pq.read_table(receipts).to_pylist() if r["outcome"] == "accepted"]
    assert decode_exact_json(accepted["processing_json"]) == {"source_fields": raw, "entry_kind": "row"}
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored.parquet", dataset=dataset, generation_id="g"
    )
    assert pq.read_table(restored).to_pylist() == [raw]


def test_scheduled_rebuild_reads_the_prior_pair_and_a_changed_returned_value_is_a_new_version(tmp_path, monkeypatch):
    """bill-family and committee-rosters resume this way: selected pair, private working rows, next pair."""
    from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup

    class Returned(SubjectReceiptRollup):
        name = "returned-columns-test"
        outputs = tuple(dataset + ".parquet" for dataset in RETURNED)

        def build(self, output_dir):
            raise NotImplementedError

    held = {}

    def builder(directory, *, download_prior):
        paths = []
        for dataset, (_, raw, _, reread) in RETURNED.items():
            path = directory / (dataset + ".parquet")
            if download_prior(path.name, path):
                held[dataset] = pq.read_table(path).to_pylist()
                shaped(path, [raw | reread])
            else:
                shaped(path, [raw])
            paths.append(path)
        return tuple(paths)

    def accepted():
        receipts = pq.read_table(tmp_path / "etl_receipts.parquet").to_pylist()
        return {r["dataset"]: r for r in receipts if r["outcome"] == "accepted"}

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    Returned().build_receipts(tmp_path, builder)
    assert not held
    before = accepted()
    outputs = Returned().build_receipts(tmp_path, builder)
    after = accepted()
    assert {path.stem for path in outputs} == set(RETURNED)
    for path in outputs:
        _, raw, _, reread = RETURNED[path.stem]
        old, new = before[path.stem], after[path.stem]
        assert held[path.stem] == [raw]
        [row] = pq.read_table(path).to_pylist()
        assert {column: row[column] for column in reread} == reread
        # Only a subject column moves the row's version; a receipt-only field would leave it standing.
        assert new["record_id"] == old["record_id"] and new["subject_version"] != old["subject_version"]
        assert decode_exact_json(new["diagnostic_json"])["prior_receipt"]["receipt_id"] == old["receipt_id"]


def test_real_roster_producer_publishes_congress_basis_and_resumes_from_its_native_pair(tmp_path, monkeypatch):
    from spicy_docs.sources.congress.committee_rosters import CommitteeRosterError

    from spicy_regs.transforms import build_committee_rosters as producer
    from tests.test_committee_rosters import StubListingReader, StubRosters

    monkeypatch.setattr(producer, "current_congress", lambda: 119)  # the House fixture states the 119th

    def build(generation, rosters, inputs):
        run = CongressBuild(generation, inputs=inputs)
        paths = producer.build_committee_rosters(
            tmp_path, reader=StubListingReader(), rosters=rosters, receipt_build=run
        )
        return run, {path.stem: path for path in paths}

    def seats(path, chamber):
        rows = pq.read_table(path).to_pylist()
        return {tuple(sorted((k, str(v)) for k, v in row.items())) for row in rows if row["chamber"] == chamber}

    first, published = build("first", StubRosters(), {})
    assignments = pq.read_table(published["committee_assignments"])
    assert assignments.schema.field("congress_basis").type == pa.string()
    assert not {"file_date", "observed_at"} & set(assignments.schema.names)
    # The House file states its Congress; the Senate file states none, so its rows say the caller supplied it.
    assert {(row["chamber"], row["congress_basis"]) for row in assignments.to_pylist()} == {
        ("house", "file"),
        ("senate", "caller"),
    }
    assert first.receipt_path is not None
    inputs = {name: CongressInput(path, first.receipt_path, "first") for name, path in published.items()}
    # No Senate file this run: its seats, and their basis, can only come from the prior pair's receipts.
    _, again = build("second", StubRosters(senate_error=CommitteeRosterError("stub: truncated")), inputs)
    kept = seats(again["committee_assignments"], "senate")
    assert kept and kept == seats(published["committee_assignments"], "senate")
    assert seats(again["committee_assignments"], "house") == seats(published["committee_assignments"], "house")


@pytest.mark.parametrize("dataset", RETURNED)
def test_bulk_restore_uses_retained_schema_for_python_mapped_datasets(tmp_path, dataset, monkeypatch):
    from spicy_regs import congress_bulk, etl_bulk

    _, raw, _, _ = RETURNED[dataset]
    source = shaped(tmp_path / "source.parquet", [raw])
    pair = write_congress_dataset(source, tmp_path / "native", dataset=dataset, generation_id="g")
    expected = restore_processing_input(*pair, tmp_path / "row.parquet", dataset=dataset, generation_id="g")
    scans = []
    restore = congress_bulk.restore_input
    monkeypatch.setattr(congress_bulk, "restore_input", lambda *a, **kw: scans.append(kw["dataset"]) or restore(*a, **kw))
    admissions = []
    admit = etl_bulk.validate_bundle
    monkeypatch.setattr(etl_bulk, "validate_bundle", lambda *a, **kw: admissions.append(1) or admit(*a, **kw))
    actual = restore_processing_input(*pair, tmp_path / "bulk.parquet", dataset=dataset, generation_id="g", bulk=True)
    assert pq.read_table(actual).equals(pq.read_table(expected), check_metadata=True)
    assert scans == [dataset] and admissions == [1]


def test_earlier_policies_reads_only_for_the_exact_roll_call_votes_descriptor():
    from dataclasses import replace
    from spicy_regs.congress_receipts import policy as congress_policy
    from spicy_regs.policy_migrations import earlier_policies

    current = congress_policy("roll_call_votes")
    admitted = earlier_policies(current)
    assert len(admitted) == 1 and admitted[0].policy_version == "congress-subjects/1"

    # A changed descriptor under the current label is not the recorded policy, so it reads nothing earlier.
    tampered = replace(current, subject_schema=current.subject_schema.remove(current.subject_schema.get_field_index("question")))
    assert earlier_policies(tampered) == ()
