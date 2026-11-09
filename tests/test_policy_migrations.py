"""A shipped policy change must say how the previous generation is read: carried or replayed (ss-aj7t)."""

import copy
from dataclasses import replace

import pyarrow as pa

from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.etl_receipts import receipt_policies
from spicy_regs.legislative_receipts import policy as legislative_policy
from spicy_regs.policy_migrations import descriptor_digest, ledger, replay_reason, unreadable_shipped_policies


def test_every_installed_policy_is_shipped_and_every_earlier_one_is_read_or_replayed():
    assert unreadable_shipped_policies() == []


def test_a_changed_policy_fails_until_recorded_then_until_its_predecessor_is_read_or_replayed():
    installed = installed_policies()
    current = installed["bill_cosponsors"]
    changed = replace(current, subject_schema=current.subject_schema.append(pa.field("added", pa.string())))
    installed["bill_cosponsors"] = changed
    migrations = copy.deepcopy(dict(ledger()))
    assert [p.split(":")[0] for p in unreadable_shipped_policies(installed, migrations)] == ["bill_cosponsors"]

    migrations["shipped"]["bill_cosponsors"] = [*migrations["shipped"]["bill_cosponsors"], descriptor_digest(changed)]
    problems = unreadable_shipped_policies(installed, migrations)
    assert len(problems) == 1 and "neither declared readable" in problems[0]

    migrations["replay_required"]["bill_cosponsors"] = {descriptor_digest(current): {"reason": " "}}
    assert "states no reason" in unreadable_shipped_policies(installed, migrations)[0]
    migrations["replay_required"]["bill_cosponsors"][descriptor_digest(current)]["reason"] = "rebuilt from originals"
    assert unreadable_shipped_policies(installed, migrations) == []


def test_summary_v2_is_identified_but_replayed_while_bill_versions_v1_carries():
    summaries = receipt_policies(legislative_policy("bill_publisher_summaries"))
    assert [p.policy_version for p in summaries] == [f"legislative-documents/{n}" for n in (3, 2, 1)]
    assert replay_reason(summaries[0]) is None and "BILLSTATUS" in (replay_reason(summaries[1]) or "")
    current, earlier = receipt_policies(legislative_policy("bill_versions"))
    assert replay_reason(current) is None and replay_reason(earlier) is None
    assert set(ledger()["replay_required"]) == {"bill_publisher_summaries"}
    # Seeded from the descriptors live generations state (2026-11-09): only these two differ from installed.
    assert {name for name, digests in ledger()["shipped"].items() if len(digests) > 1} == {
        "bill_versions", "bill_publisher_summaries"}
