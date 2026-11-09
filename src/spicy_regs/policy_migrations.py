"""Every dataset policy a generation was published under, and how each earlier one is still read (ss-aj7t).

``policy_migrations.json`` lists, per dataset, the descriptor digests shipped in order; the last is the
installed ``etl_policies`` file. It was seeded from the policies the live generations' roots declare
(2026-11-09), not from git history: a descriptor that never reached a publication needs no reader. An earlier shipped descriptor is either declared readable by
``earlier_receipt_policies`` (its rows carry forward under their own receipts) or named in
``replay_required`` with a reason: its rows cannot become current rows, so the next run rebuilds them from
retained originals. ``unreadable_shipped_policies`` is the CI guard: a policy change that does neither fails
before a nightly meets the old generation.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from functools import cache
from importlib.resources import files

LEDGER = "policy_migrations.json"


class PriorReplayRequired(ValueError):
    """A selected prior is under a shipped policy whose rows must be rebuilt from originals, not carried."""

    def __init__(self, dataset: str, policy_version: str, reason: str):
        super().__init__(f"{dataset} prior is {policy_version} and must be replayed from originals: {reason}")
        self.dataset, self.policy_version, self.reason = dataset, policy_version, reason


def descriptor_digest(policy) -> str:
    return "sha256:" + hashlib.sha256(
        json.dumps(policy.descriptor(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@cache
def ledger() -> Mapping:
    return json.loads(files("spicy_regs").joinpath(LEDGER).read_text())


def replay_reason(policy, migrations: Mapping | None = None) -> str | None:
    """Why rows selected under this exact policy are rebuilt rather than carried; ``None`` when they carry."""
    entry = (migrations or ledger())["replay_required"].get(policy.dataset, {})
    return entry.get(descriptor_digest(policy), {}).get("reason")


def unreadable_shipped_policies(installed: Mapping | None = None, migrations: Mapping | None = None) -> list[str]:
    """Each installed policy missing from the ledger, and each shipped one neither readable nor replay-required."""
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.etl_receipts import receipt_policies

    installed = installed_policies() if installed is None else installed
    migrations = ledger() if migrations is None else migrations
    problems = []
    for name, policy in sorted(installed.items()):
        shipped = migrations["shipped"].get(name, [])
        current = descriptor_digest(policy)
        if not shipped or shipped[-1] != current:
            problems.append(f"{name}: installed {policy.policy_version} ({current}) is not the last shipped "
                            f"descriptor in {LEDGER}; append it and declare how the previous one is read")
            continue
        readable = {descriptor_digest(p) for p in receipt_policies(policy)}
        replay = migrations["replay_required"].get(name, {})
        for digest in shipped[:-1]:
            if digest in replay:
                if not str(replay[digest].get("reason", "")).strip():
                    problems.append(f"{name}: replay-required {digest} states no reason")
            elif digest not in readable:
                problems.append(f"{name}: shipped {digest} is neither declared readable in earlier_receipt_policies "
                                "nor listed in replay_required with a reason")
    return problems
