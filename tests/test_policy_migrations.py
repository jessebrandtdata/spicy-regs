"""A shipped policy change must say how the previous generation is read: carried or replayed (ss-aj7t, ss-ag1j)."""

import copy
import json
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.etl_receipts import DatasetPolicy, receipt_policies
from spicy_regs.legislative_receipts import policy as legislative_policy
from spicy_regs.policy_migrations import (
    REPLAY_REQUIRED, UNREAD, descriptor_digest, earlier_policies, earlier_policy, ledger, replay_reason, shipped,
    unreadable_shipped_policies,
)
from spicy_regs.transforms.regulations_receipts import policy as regulations_policy

FIXTURES = Path(__file__).parent / "fixtures/policy_migrations"


def _fixture(name):
    return json.loads((FIXTURES / name).read_text())


def test_every_installed_policy_is_recorded_current_and_every_earlier_entry_is_well_formed():
    assert unreadable_shipped_policies() == []


def _frozen_answer_problems(installed):
    """Each frozen answer of the deleted ladder the ledger no longer records, or answers differently.

    The ladder was asked about 350 policies (every installed one, every history-file descriptor, every policy it
    returned) before it was deleted; its answers are frozen in the fixture. Recording a new policy adds to the ledger
    and must leave every frozen one recorded and answered as before. Deviations are the 24 descriptors no installed
    policy reaches, each with why.
    """
    from spicy_regs import policy_migrations

    frozen = _fixture("earlier_receipt_policies_20261110.json")
    recorded = {}
    for name, policy in installed.items():
        recorded[descriptor_digest(policy)] = policy
        for declared in policy_migrations.ledger()[name].get("earlier", ()):
            earlier = earlier_policy(name, declared["policy_version"])
            recorded[descriptor_digest(earlier)] = earlier
    answers, deviations = frozen["answers"], frozen["deviations"]
    problems = [f"{dataset} {version} is no longer recorded" for digest, (dataset, version, _) in answers.items()
                if digest not in recorded]
    for digest, (dataset, version, earlier) in answers.items():
        if digest in recorded:
            policy = recorded[digest]
            expected = deviations[digest]["earlier"] if digest in deviations else earlier
            if ((policy.dataset, policy.policy_version) != (dataset, version)
                    or [descriptor_digest(p) for p in earlier_policies(policy)] != expected):
                problems.append(f"{dataset} {version} reads differently")
    return problems


def test_the_ledger_answers_what_the_retired_ladder_answered():
    frozen = _fixture("earlier_receipt_policies_20261110.json")
    assert set(frozen["deviations"]) <= set(frozen["answers"]) and all(d["why"] for d in frozen["deviations"].values())
    assert _frozen_answer_problems(installed_policies()) == []
    # The installed policies of that day were answered exactly as before; only unreachable chain members deviate.
    assert not {descriptor_digest(p) for p in installed_policies().values()} & set(frozen["deviations"])


@pytest.fixture
def staged_ledger(monkeypatch):
    """Serve a ledger the generator wrote, to every lookup, for one test."""
    from spicy_regs import policy_migrations

    def serve(path):
        monkeypatch.setattr(policy_migrations, "ledger", lambda: json.loads(path.read_text()))
        policy_migrations._recorded.cache_clear()
    yield serve
    policy_migrations._recorded.cache_clear()


def test_a_promotion_the_generator_records_keeps_every_frozen_answer(tmp_path, staged_ledger):
    """The next column promotion must not fail the frozen answers: they are a subset, not the whole ledger."""
    from scripts.update_etl_policy_declarations import record_migrations

    ledger_path, root = _staged(tmp_path, "federal_register")
    current = installed_policies()["federal_register"]
    promoted = replace(current, subject_schema=current.subject_schema.append(pa.field("added", pa.string())),
                       policy_version="regulations-native-v9")
    record_migrations({"federal_register": promoted.descriptor()}, {}, ledger_path=ledger_path, installed=root)
    staged_ledger(ledger_path)
    installed = {**installed_policies(), "federal_register": promoted}
    assert _frozen_answer_problems(installed) == []
    assert [p.policy_version for p in earlier_policies(promoted)] == [
        "regulations-native-v3", "regulations-native-v2", "regulations-native-v1"]
    # Dropping a recorded policy, or reordering one, still fails.
    edited = json.loads(ledger_path.read_text())
    edited["federal_register"]["earlier"].pop(0)
    ledger_path.write_text(json.dumps(edited))
    staged_ledger(ledger_path)
    assert "federal_register regulations-native-v1 is no longer recorded" in _frozen_answer_problems(installed)


def test_no_published_prior_becomes_unreadable():
    """Each policy a live family root declared on 2026-11-10 is recorded in the ledger and read by the code.

    An installed policy reads it. The two regulatory-checkpoints logs have no installed policy file: the checkpoint
    writer builds theirs (``checkpoint_policy``), and the ledger records exactly that policy (ss-p6yh; before it, the
    live check ``python -m spicy_regs.policy_migrations`` named both).
    """
    from spicy_regs.transforms.regulations_checkpoints import checkpoint_policy

    installed = installed_policies()
    built_by_code = []
    for family, declared in _fixture("live_declared_20261110.json")["families"].items():
        for dataset, version, digest in declared:
            assert digest in shipped(dataset), (family, dataset, version)
            if dataset not in installed:
                built_by_code.append((family, dataset))
                assert digest == descriptor_digest(checkpoint_policy(dataset)), (family, dataset, version)
                continue
            readable = {descriptor_digest(p): p for p in receipt_policies(installed[dataset])}
            assert digest in readable and replay_reason(readable[digest]) is None, (family, dataset, version)
    assert built_by_code == [("regulatory-checkpoints", "failed_keys"),
                             ("regulatory-checkpoints", "pending_comment_text")]


def test_a_changed_policy_fails_until_recorded_with_its_predecessor_as_an_earlier_entry():
    installed = installed_policies()
    current = installed["bill_cosponsors"]
    changed = replace(current, subject_schema=current.subject_schema.append(pa.field("added", pa.string())),
                      policy_version="congress-subjects/9")
    installed["bill_cosponsors"] = changed
    migrations = copy.deepcopy(dict(ledger()))
    [problem] = unreadable_shipped_policies(installed, migrations)
    assert problem.startswith("bill_cosponsors: installed congress-subjects/9") and "not recorded" in problem

    migrations["bill_cosponsors"] = {"earlier": [current.descriptor()], "installed": descriptor_digest(changed)}
    assert unreadable_shipped_policies(installed, migrations) == []

    # A changed descriptor under the label it shipped under is two policies with one name.
    same_label = replace(changed, policy_version=current.policy_version)
    installed["bill_cosponsors"] = same_label
    migrations["bill_cosponsors"]["installed"] = descriptor_digest(same_label)
    assert "share a policy_version" in unreadable_shipped_policies(installed, migrations)[0]

    # Replay-required needs code that replays it: a withheld prior must never read as "nothing published".
    installed["bill_cosponsors"] = changed
    migrations["bill_cosponsors"] = {"earlier": [{**current.descriptor(), REPLAY_REQUIRED: "rebuilt"}],
                                     "installed": descriptor_digest(changed)}
    assert "no code replays it" in unreadable_shipped_policies(installed, migrations)[0]

    migrations["bill_cosponsors"]["earlier"][0]["dataset"] = "bill_actions"
    assert "not a descriptor of this dataset" in unreadable_shipped_policies(installed, migrations)[0]


def test_a_registered_replay_still_states_its_reason():
    migrations = copy.deepcopy(dict(ledger()))
    migrations["bill_publisher_summaries"]["earlier"][1][REPLAY_REQUIRED] = " "
    assert ["states no reason" in p for p in unreadable_shipped_policies(migrations=migrations)] == [True]


def test_summary_v2_is_identified_but_replayed_while_bill_versions_v1_carries():
    summaries = receipt_policies(legislative_policy("bill_publisher_summaries"))
    assert [p.policy_version for p in summaries] == [f"legislative-documents/{n}" for n in (3, 2)]
    assert replay_reason(summaries[0]) is None and "BILLSTATUS" in (replay_reason(summaries[1]) or "")
    current, earlier = receipt_policies(legislative_policy("bill_versions"))
    assert replay_reason(current) is None and replay_reason(earlier) is None
    assert {name for name, entry in ledger().items()
            if any(REPLAY_REQUIRED in e for e in entry.get("earlier", ()))} == {"bill_publisher_summaries"}
    # The two Register tables' column promotion (ss-vse3) carries: each earlier row restores from its retained
    # input, which already holds the promoted values.
    for name in ("federal_register", "fr_docket_links"):
        assert all(replay_reason(p) is None for p in receipt_policies(regulations_policy(name)))


def test_lookup_is_by_exact_descriptor():
    current = installed_policies()["federal_register"]
    v2 = earlier_policy("federal_register", "regulations-native-v2")
    assert [p.policy_version for p in earlier_policies(current)] == ["regulations-native-v2", "regulations-native-v1"]
    assert [p.policy_version for p in earlier_policies(v2)] == ["regulations-native-v1"]
    # A descriptor the ledger does not record reads nothing, whatever its label.
    assert earlier_policies(replace(current, identity_fields=("document_number",))) == ()
    assert earlier_policies(DatasetPolicy("unrecorded", pa.schema([]), (), ("raw",), receipt_only=True)) == ()
    with pytest.raises(KeyError, match="no earlier policy"):
        earlier_policy("federal_register", "regulations-native-v3")


def test_an_unread_entry_keeps_its_shape_but_is_neither_read_nor_shipped():
    summaries = installed_policies()["bill_publisher_summaries"]
    first = earlier_policy("bill_publisher_summaries", "legislative-documents/1")
    assert descriptor_digest(first) not in {descriptor_digest(p) for p in receipt_policies(summaries)}
    assert descriptor_digest(first) not in shipped("bill_publisher_summaries")
    assert earlier_policy("committee_reports", "legislative-documents/1").identity_fields == ("package_id", "part_id")
    migrations = copy.deepcopy(dict(ledger()))
    migrations["committee_reports"]["earlier"][0][UNREAD] = ""
    assert ["committee_reports: unread" in p for p in unreadable_shipped_policies(migrations=migrations)] == [True]


def test_live_check_names_a_published_policy_missing_from_the_ledger(monkeypatch):
    from spicy_regs import policy_migrations
    from spicy_regs.sources import publication

    recorded = installed_policies()["bill_cosponsors"]
    unrecorded = replace(recorded, policy_version="fixture/9")
    roots = {"a": [recorded.descriptor()], "b": [recorded.descriptor(), unrecorded.descriptor()]}
    monkeypatch.setattr(publication, "current_index", lambda base: {"families": {name: name for name in roots}})
    monkeypatch.setattr(publication, "load_family_root",
                        lambda base, entry: (b"", {"spec": {"etlReceipts": {"policies": roots[entry]}}}))
    [problem] = policy_migrations.published_policies_unrecorded("https://example.invalid")
    assert problem.startswith("b: published bill_cosponsors fixture/9")


def _staged(tmp_path, name):
    """A copy of the ledger and of one installed declaration, for the generator to rewrite."""
    from spicy_regs.policy_migrations import LEDGER

    root = tmp_path / "installed"
    root.mkdir()
    source = Path(__file__).parents[1] / "src/spicy_regs"
    (root / f"{name}.json").write_text((source / f"etl_policies/{name}.json").read_text())
    (tmp_path / LEDGER).write_text((source / LEDGER).read_text())
    return tmp_path / LEDGER, root


def test_the_generator_records_a_promotion_with_its_predecessor_as_read(tmp_path):
    from scripts.update_etl_policy_declarations import record_migrations

    ledger_path, root = _staged(tmp_path, "bill_cosponsors")
    current = installed_policies()["bill_cosponsors"]
    promoted = replace(current, subject_schema=current.subject_schema.append(pa.field("added", pa.string())),
                       policy_version="congress-subjects/9")
    # A changed schema under the label it shipped under is refused: receipts name a policy by its version.
    with pytest.raises(ValueError, match="changed under congress-subjects/1; give it a new policy_version"):
        record_migrations({"bill_cosponsors": replace(promoted, policy_version=current.policy_version).descriptor()},
                          {}, ledger_path=ledger_path, installed=root)
    record_migrations({"bill_cosponsors": promoted.descriptor()}, {}, ledger_path=ledger_path, installed=root)
    recorded = json.loads(ledger_path.read_text())
    assert recorded["bill_cosponsors"] == {"earlier": [current.descriptor()], "installed": descriptor_digest(promoted)}
    installed = {**installed_policies(), "bill_cosponsors": promoted}
    assert unreadable_shipped_policies(installed, recorded) == []
    # Unchanged on a second run; the rest of the ledger untouched.
    record_migrations({"bill_cosponsors": promoted.descriptor()}, {}, ledger_path=ledger_path, installed=root)
    assert json.loads(ledger_path.read_text()) == recorded == {**ledger(), "bill_cosponsors": recorded["bill_cosponsors"]}


def test_the_generator_marks_a_replay_only_when_asked_and_only_for_a_change(tmp_path):
    from scripts.update_etl_policy_declarations import record_migrations

    ledger_path, root = _staged(tmp_path, "bill_cosponsors")
    current = installed_policies()["bill_cosponsors"]
    promoted = replace(current, policy_version="congress-subjects/9",
                       subject_schema=current.subject_schema.append(pa.field("added", pa.string(), nullable=False)))
    with pytest.raises(ValueError, match="names datasets whose policy did not change: \\['bill_actions'\\]"):
        record_migrations({"bill_cosponsors": promoted.descriptor()}, {"bill_actions": "x"},
                          ledger_path=ledger_path, installed=root)
    record_migrations({"bill_cosponsors": promoted.descriptor()}, {"bill_cosponsors": "the new column is NOT NULL"},
                      ledger_path=ledger_path, installed=root)
    [earlier] = json.loads(ledger_path.read_text())["bill_cosponsors"]["earlier"]
    assert earlier[REPLAY_REQUIRED] == "the new column is NOT NULL"


def test_a_marker_landing_with_promoted_columns_still_reads_the_prior(tmp_path, staged_ledger, monkeypatch):
    """The plan's route for bill-family and the small Congress families: one change promotes columns and marks the
    table retired, and that night's run still restores its prior through receipts under the new policy.

    Before, the generator and the guard skipped a marked table, so the promoted policy recorded nothing earlier and
    that last receipts read refused its own prior.
    """
    from scripts.update_etl_policy_declarations import record_migrations
    from spicy_regs import subject_catalog

    ledger_path, root = _staged(tmp_path, "bill_cosponsors")
    current = installed_policies()["bill_cosponsors"]
    promoted = replace(current, subject_schema=current.subject_schema.append(pa.field("source_sha256", pa.string())),
                       policy_version="congress-subjects/9")
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset({"bill_cosponsors"}))
    record_migrations({"bill_cosponsors": {**promoted.descriptor(), subject_catalog.RECEIPTS_RETIRED: True}}, {},
                      ledger_path=ledger_path, installed=root)
    staged_ledger(ledger_path)
    assert [descriptor_digest(p) for p in receipt_policies(promoted)] == [
        descriptor_digest(promoted), descriptor_digest(current)]
    installed = {**installed_policies(), "bill_cosponsors": promoted}
    assert unreadable_shipped_policies(installed, json.loads(ledger_path.read_text())) == []
