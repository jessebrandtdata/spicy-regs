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
    REPLAY_REQUIRED, descriptor_digest, earlier_policies, earlier_policy, ledger, replay_reason, shipped,
    unreadable_shipped_policies,
)
from spicy_regs.transforms.regulations_receipts import policy as regulations_policy

FIXTURES = Path(__file__).parent / "fixtures/policy_migrations"


def _fixture(name):
    return json.loads((FIXTURES / name).read_text())


def test_every_installed_policy_is_recorded_current_and_every_earlier_entry_is_well_formed():
    assert unreadable_shipped_policies() == []


def test_the_ledger_answers_what_the_retired_ladder_answered():
    """Every installed policy, every history-file descriptor and every policy the ladder returned (350 asked).

    The answers were frozen from ``earlier_receipt_policies.earlier_policies`` before it was deleted, so a later
    ledger edit that changes what any of them reads fails here. The ledger stores no digest of its own answer:
    the fixture's are the ladder's. Deviations are the 24 descriptors no installed policy reaches, each with why.
    """
    frozen = _fixture("earlier_receipt_policies_20261110.json")
    policies = {}
    for name, policy in installed_policies().items():
        policies[descriptor_digest(policy)] = policy
        policies.update((descriptor_digest(p), p) for p in earlier_policies(policy))
    answers, deviations = frozen["answers"], frozen["deviations"]
    assert set(deviations) <= set(answers) and all(d["why"] for d in deviations.values())
    reached = set()
    for digest, (dataset, version, earlier) in answers.items():
        if digest not in policies:
            # Asked of the ladder but recorded nowhere now: its two unreachable /1 history entries.
            assert (dataset, version, earlier) in [("bill_publisher_summaries", "legislative-documents/1", []),
                                                   ("committee_reports", "legislative-documents/1", [])]
            continue
        policy = policies[digest]
        assert (policy.dataset, policy.policy_version) == (dataset, version)
        expected = deviations[digest]["earlier"] if digest in deviations else earlier
        assert [descriptor_digest(p) for p in earlier_policies(policy)] == expected, (dataset, version)
        reached.add(digest)
    # Every installed policy is answered exactly as before; only unreachable chain members deviate.
    assert {descriptor_digest(p) for p in installed_policies().values()} <= reached - set(deviations)
    assert not {descriptor_digest(p) for p in installed_policies().values()} & set(deviations)


def test_the_retired_ladder_still_answers_as_frozen():
    """Holds the fixture to the ladder it was taken from, while both exist: asked about the same 350 policies."""
    from importlib.resources import files

    from spicy_regs.earlier_receipt_policies import earlier_policies as ladder

    pending = list(installed_policies().values())
    for package, name in [("spicy_regs", f"{n}_policy_history.json") for n in (
            "citation_navigation", "crs_detail", "fec_conversion", "fec", "government", "legislative",
            "native_legal", "navigation", "source_context")] + [("spicy_regs.scorecards", n) for n in (
            "geography_policy_history.json", "historical_rating_policies.json", "member_link_policy_history.json")]:
        history = json.loads(files(package).joinpath(name).read_text())
        pending += [DatasetPolicy.from_descriptor(d) for d in ([history] if "dataset" in history else history.values())]
    answered = {}
    while pending:
        policy = pending.pop()
        if (digest := descriptor_digest(policy)) not in answered:
            answered[digest] = [descriptor_digest(p) for p in ladder(policy)]
            pending += ladder(policy)
    frozen = _fixture("earlier_receipt_policies_20261110.json")["answers"]
    assert answered == {digest: earlier for digest, (_, _, earlier) in frozen.items()}


def test_no_published_prior_becomes_unreadable():
    """Each policy a live family root declared on 2026-11-10 is installed, or read by the installed policy.

    The two regulatory-checkpoints logs are not installed policies and were missing from the ledger before this
    change too (``python -m spicy_regs.policy_migrations`` names them).
    """
    installed = installed_policies()
    unrecorded = []
    for family, declared in _fixture("live_declared_20261110.json")["families"].items():
        for dataset, version, digest in declared:
            if dataset not in installed:
                unrecorded.append((family, dataset))
                continue
            assert digest in shipped(dataset), (family, dataset, version)
            readable = {descriptor_digest(p): p for p in receipt_policies(installed[dataset])}
            assert digest in readable and replay_reason(readable[digest]) is None, (family, dataset, version)
    assert unrecorded == [("regulatory-checkpoints", "failed_keys"), ("regulatory-checkpoints", "pending_comment_text")]


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
    migrations["bill_publisher_summaries"]["earlier"][0][REPLAY_REQUIRED] = " "
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
