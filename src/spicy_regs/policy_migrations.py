"""Every dataset policy a generation was published under, and how each earlier one is still read (ss-aj7t).

``policy_migrations.json`` lists, per dataset, the descriptor digests shipped in order; the last is the
installed ``etl_policies`` file. It was seeded from the policies the live generations' roots declare
(2026-11-09), not from git history: a descriptor that never reached a publication needs no reader. An earlier
shipped descriptor is either declared readable by ``earlier_receipt_policies`` (its rows carry forward under
their own receipts) or named in ``replay_required`` with a reason: its rows cannot become current rows, so the
next run rebuilds them from retained originals. ``unreadable_shipped_policies`` is the CI guard: a policy
change that does neither fails before a nightly meets the old generation.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from importlib.resources import files

LEDGER = "policy_migrations.json"


@dataclass(frozen=True)
class Replay:
    """The code that rebuilds a dataset from originals when its selected prior is replay-required.

    ``owner`` is ``module:attribute`` naming that code's own constant for the dataset, so a registration cannot
    outlive the replay. ``max_dropped_rows`` bounds how many prior rows the rebuild may leave out (a bill the
    reader refuses or the zip no longer holds, a revision the XML no longer lists) before the run refuses.
    """

    owner: str
    max_dropped_rows: int
    bound_basis: str


#: Only these datasets may be replay-required: any other withheld prior would read as "nothing published"
#: to a caller of ``SelectedPriors.get`` that has no replay.
REPLAYS: Mapping[str, Replay] = {
    "bill_publisher_summaries": Replay(
        "spicy_regs.transforms.build_bill_family:REPLAYED_FROM_STATUS",
        max_dropped_rows=25,
        bound_basis="the full 108-119 replay of 2026-11-09 restated 190,155 of the 190,156 live rows; the one it "
                    "left out (119-hr-1455 00, 2025-03-21) is an older revision the XML no longer lists, and one "
                    "new revision appeared the same day. 25 allows a few weeks of such drift and no more",
    ),
}


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
        if replay and not _registered(name):
            problems.append(f"{name}: replay-required in {LEDGER} but no code replays it (policy_migrations.REPLAYS)")
        for digest in shipped[:-1]:
            if digest in replay:
                if not str(replay[digest].get("reason", "")).strip():
                    problems.append(f"{name}: replay-required {digest} states no reason")
            elif digest not in readable:
                problems.append(f"{name}: shipped {digest} is neither declared readable in earlier_receipt_policies "
                                "nor listed in replay_required with a reason")
    return problems


def _registered(dataset: str) -> bool:
    replay = REPLAYS.get(dataset)
    if replay is None:
        return False
    module, _, attribute = replay.owner.partition(":")
    return getattr(importlib.import_module(module), attribute, None) == dataset


def dropped_replay_rows(dataset: str, identity_fields, prior_subjects, rebuilt) -> list[dict]:
    """Prior identities the rebuilt table no longer holds, compared as text so native and source types agree."""
    import duckdb

    def keys(paths):
        listed = ", ".join("'" + str(path).replace("'", "''") + "'" for path in paths)
        columns = ", ".join(f'CAST("{name}" AS VARCHAR) AS "{name}"' for name in identity_fields)
        return f"SELECT DISTINCT {columns} FROM read_parquet([{listed}])"

    with duckdb.connect() as con:
        cursor = con.execute(f"SELECT * FROM ({keys(prior_subjects)}) EXCEPT SELECT * FROM ({keys([rebuilt])}) "
                             f"ORDER BY ALL")
        return [dict(zip(identity_fields, row, strict=True)) for row in cursor.fetchall()]


def published_policies_unrecorded(base_url: str) -> list[str]:
    """Live check: each policy a published family root declares must be a shipped descriptor in the ledger."""
    from spicy_regs.etl_receipts import DatasetPolicy
    from spicy_regs.sources import publication
    from spicy_regs.subject_catalog import SHARED_LOG

    index = publication.current_index(base_url)
    problems = []
    for family, entry in sorted(index["families"].items()):
        _, root = publication.load_family_root(base_url, entry)
        for declared in root.get("spec", {}).get("etlReceipts", {}).get("policies", []):
            policy = DatasetPolicy.from_descriptor({k: v for k, v in declared.items() if k != SHARED_LOG})
            if descriptor_digest(policy) not in ledger()["shipped"].get(policy.dataset, []):
                problems.append(f"{family}: published {policy.dataset} {policy.policy_version} "
                                f"({descriptor_digest(policy)}) is not in {LEDGER}")
    return problems


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Check every published policy is a shipped descriptor.")
    parser.add_argument("--base-url", required=True)
    found = published_policies_unrecorded(parser.parse_args().base_url)
    print("\n".join(found) or "Every published policy is recorded.")
    sys.exit(1 if found else 0)
