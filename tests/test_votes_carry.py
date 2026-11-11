"""The vote writer carries receipts: unchanged rows keep theirs, changed rows name their predecessor, size stays flat."""

import json

import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk
from spicy_regs.congress_receipts import CongressInput
from spicy_regs.etl_receipts import decode_exact_json
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors, SubjectReceiptRollup
from spicy_regs.votes_batch_receipts import restore_processing_input, write_congress_dataset
from tests.test_congress_receipts import shaped
from tests.test_subject_receipt_rollups import select_bundle
from tests.test_votes_batch_current import WITNESS

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")

TABLE = [("member_votes", "position"), ("member_vote_terms", "term_match")]


def _rows(dataset, count, changed=()):
    if dataset == "member_votes":
        return [{"vote_id": f"119-house-{i // 50}", "member_key": f"name:M{i}", "congress": "119", "chamber": "house",
                 "position": "Nay" if i in changed else "Yea"} for i in range(count)]
    return [{"vote_id": f"119-house-{i // 50}", "member_key": f"name:M{i}", "chamber": "house",
             "bioguide_id": f"B{i:05d}", "vote_day": "2025-01-23",
             "term_match": "half_open" if i not in changed else "unresolved_member", "term_index": "1",
             "term_start": "2025-01-03", "term_end": "2027-01-03"} for i in range(count)]


def _night(tmp_path, dataset, name, rows, prior=None, generation=None, **kwargs):
    source = shaped(tmp_path / f"{name}.parquet", rows, {b"scope": b"119"})
    subject, receipts = write_congress_dataset(
        source, tmp_path / name, dataset=dataset, generation_id=generation or name, prior=prior, **kwargs)
    return source, subject, receipts


def _accepted(path):
    return {r["record_id"]: r for r in pq.read_table(path).to_pylist() if r["outcome"] == "accepted"}


@pytest.mark.parametrize(("dataset", "column"), TABLE)
def test_unchanged_rows_keep_their_receipt_and_changed_rows_name_their_predecessor(tmp_path, dataset, column):
    _, subject, receipts = _night(tmp_path, dataset, "night1", _rows(dataset, 120), witnesses=[WITNESS])
    prior = CongressInput(subject, receipts, "night1")
    changed = {3, 77}
    source, subject2, receipts2 = _night(
        tmp_path, dataset, "night2", _rows(dataset, 121, changed), prior=prior, witnesses=[WITNESS])  # one row added
    before, after = _accepted(receipts), _accepted(receipts2)
    assert len(after) == 121
    changed_ids = {k for k, r in after.items() if k in before and r != before[k]}
    assert len(changed_ids) == 2
    for key, receipt in after.items():
        if key not in before:
            assert receipt["generation_id"] == "night2" and "prior_receipt" not in decode_exact_json(receipt["diagnostic_json"])
        elif key in changed_ids:
            reference = decode_exact_json(receipt["diagnostic_json"])["prior_receipt"]
            assert reference["receipt_id"] == before[key]["receipt_id"]
            assert reference["generation_id"] == "night1"
            assert receipt["generation_id"] == "night2" and receipt["witnesses"] == _rows_witnesses(receipt)
        else:
            assert receipt == before[key]  # byte-identical, original generation and witnesses
    # The metadata observation is unchanged, so it is carried too.
    assert [r for r in pq.read_table(receipts2).to_pylist() if r["outcome"] == "observed"] == [
        r for r in pq.read_table(receipts).to_pylist() if r["outcome"] == "observed"]
    # The next night restores exactly the source this one was built from.
    restored = restore_processing_input(subject2, receipts2, tmp_path / "restored.parquet", dataset=dataset,
                                        generation_id="night2")
    assert pq.read_table(restored).equals(pq.read_table(source), check_metadata=True)


def _rows_witnesses(receipt):
    """A changed row's witnesses are its own (source plus the caller's), never its predecessor's."""
    assert [w["source_id"] for w in receipt["witnesses"]] == ["shaped-observation:" + receipt["dataset"], "capture"]
    return receipt["witnesses"]


@pytest.mark.parametrize(("dataset", "column"), TABLE)
def test_receipt_size_stays_flat_across_nights(tmp_path, dataset, column):
    rows = 300
    _, subject, receipts = _night(tmp_path, dataset, "n0", _rows(dataset, rows), witnesses=[WITNESS])
    first = receipts.stat().st_size
    sizes = []
    for night in range(1, 7):
        prior = CongressInput(subject, receipts, f"n{night - 1}")
        changed = {night, night + 50, night + 100}
        _, subject, receipts = _night(tmp_path, dataset, f"n{night}", _rows(dataset, rows, changed), prior=prior,
                                      witnesses=[WITNESS])
        sizes.append(receipts.stat().st_size)
    longest = max(len(r["diagnostic_json"]) + len(json.dumps(r["witnesses"]))
                  for r in pq.read_table(receipts).to_pylist())
    assert max(sizes) < first * 1.2
    assert longest < 1500
    # Rows changed once per night span several nights, yet no receipt holds more than one predecessor reference.
    for receipt in pq.read_table(receipts).to_pylist():
        assert "prior_receipts" not in decode_exact_json(receipt["diagnostic_json"])
        assert len(receipt["witnesses"]) <= 2


@pytest.mark.parametrize("validate", [True, False])
def test_validate_false_leaves_admission_to_the_caller(tmp_path, monkeypatch, validate):
    _, subject, receipts = _night(tmp_path, "member_votes", "n0", _rows("member_votes", 20))
    calls = []
    real = etl_bulk.validate_bundle
    monkeypatch.setattr(etl_bulk, "validate_bundle", lambda *a, **k: calls.append(1) or real(*a, **k))
    _night(tmp_path, "member_votes", "n1", _rows("member_votes", 20, {1}), prior=CongressInput(subject, receipts, "n0"),
           validate=validate)
    restore_processing_input(subject, receipts, tmp_path / "r.parquet", dataset="member_votes", generation_id="n0",
                             validate=validate)
    assert len(calls) == (3 if validate else 0)


class VotesRollup(SubjectReceiptRollup):
    name = "votes-test"
    outputs = ("member_votes.parquet",)

    def build(self, output_dir):
        raise NotImplementedError


def _builder(rows):
    def build(directory, *, download_prior):
        path = directory / "member_votes.parquet"
        if download_prior(path.name, path):  # the restored prior, exactly as the previous night's source
            assert pq.read_table(path).num_rows == 60
        shaped(path, rows)
        return path
    return build


def test_votes_rollup_admits_each_pair_once_and_carries(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    first_dir = tmp_path / "first"
    first_dir.mkdir()
    first = VotesRollup()
    first.build_receipts(first_dir, _builder(_rows("member_votes", 60)))
    select_bundle(monkeypatch, first_dir, first)

    calls = []
    real = etl_bulk.validate_bundle
    monkeypatch.setattr(etl_bulk, "validate_bundle", lambda *a, **k: calls.append(k.get("generation_id")) or real(*a, **k))
    second_dir = tmp_path / "second"
    second_dir.mkdir()
    second = VotesRollup()
    second._defer_native_selection = True  # a scheduled run: build_generation admits the finished pair
    second.build_receipts(second_dir, _builder(_rows("member_votes", 60, {5})))
    # The selected prior is admitted once; the new pair is left to the generation admission.
    assert calls == [first.receipt_generation_id]
    before, after = _accepted(first_dir / "etl_receipts.parquet"), _accepted(second_dir / "etl_receipts.parquet")
    assert sum(after[k] == before[k] for k in after) == 59
    assert {r["generation_id"] for r in after.values()} == {first.receipt_generation_id, second.receipt_generation_id}
    # The carried pair is admissible under the new generation, and the night after restores from it.
    subjects = {p.dataset: [second_dir / "member_votes.parquet"] if p.dataset == "member_votes" else []
                for p in second.receipt_policies}
    etl_bulk.validate_bundle(subjects,
                             [second_dir / "etl_receipts.parquet"], second.receipt_policies,
                             generation_id=second.receipt_generation_id)
    select_bundle(monkeypatch, second_dir, second)
    restored = SelectedPriors(tmp_path / "third").get("member_votes")
    assert pq.read_table(restored).to_pylist() == pq.read_table(
        next(second_dir.glob(".builds/*/source-output/member_votes.parquet"))).to_pylist()


@pytest.mark.parametrize("bulk", [False, True])
def test_fresh_refusal_is_reported_before_carry_can_reuse_old_receipt(tmp_path, bulk):
    rows = [{"vote_id": None, "member_key": "k"}]
    _, subject, receipts = _night(tmp_path, "member_votes", "prior", rows, bulk=bulk)
    failures = []
    _, _, current = _night(tmp_path, "member_votes", "next", rows, bulk=bulk,
                           prior=CongressInput(subject, receipts, "prior"),
                           on_fresh_failure=lambda: failures.append("refused"))
    assert failures == ["refused"]
    previous = next(r for r in pq.read_table(receipts).to_pylist() if r["outcome"] == "refused")
    assert previous in pq.read_table(current).to_pylist()
