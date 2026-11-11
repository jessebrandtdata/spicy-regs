"""Every dataset policy a generation was published under, and how each earlier one is still read (ss-aj7t, ss-ag1j).

``policy_migrations.json`` holds, per installed dataset, the digest of its installed ``etl_policies`` file and,
oldest first, the whole descriptor of every policy it was published under before. The installed policy reads each
earlier one, and an earlier one reads those before it (``earlier_policies``): a row selected under one is admitted
and carried forward under its own receipt. Two markers may stand beside a descriptor, each a reason, left out of
its digest (like ``subject_catalog.SHARED_LOG``):

- ``replay_required``: still recognised, but its rows cannot become current rows; the next run rebuilds them from
  retained originals (``REPLAYS``).
- ``unread``: no later policy reads it, so a prior under it is refused; it is recorded so a reader of a generation
  published under it can still check that generation's shape (``earlier_policy``).

The shipped policies are the earlier entries that are not ``unread``, then the installed one. This replaced a
316-line ladder of per-version branches over twelve history files (``earlier_receipt_policies``); its answers are
frozen in ``tests/fixtures/policy_migrations``. ``unreadable_shipped_policies`` is the CI guard: an installed policy
the ledger does not record as current fails before a nightly meets the old generation
(``scripts/update_etl_policy_declarations.py`` records it), and ``published_policies_unrecorded`` is the live check.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from spicy_regs.etl_receipts import DatasetPolicy

LEDGER = "policy_migrations.json"
#: The keys an earlier entry may carry beside its ``DatasetPolicy.descriptor()``; each states a reason.
REPLAY_REQUIRED = "replay_required"
UNREAD = "unread"
MARKERS = frozenset({REPLAY_REQUIRED, UNREAD})


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


class _Entry(NamedTuple):
    digest: str
    policy: DatasetPolicy
    replay: str | None
    unread: str | None


def _entries(entry: Mapping) -> tuple[_Entry, ...]:
    """Each earlier entry of one dataset's ledger entry, oldest first."""
    from spicy_regs.etl_receipts import DatasetPolicy

    found = []
    for declared in entry.get("earlier", ()):
        policy = DatasetPolicy.from_descriptor({k: v for k, v in declared.items() if k not in MARKERS})
        found.append(_Entry(descriptor_digest(policy), policy, declared.get(REPLAY_REQUIRED), declared.get(UNREAD)))
    return tuple(found)


@cache
def _recorded(dataset: str) -> tuple[str | None, tuple[_Entry, ...]]:
    entry = ledger().get(dataset, {})
    return entry.get("installed"), _entries(entry)


def earlier_policies(policy: DatasetPolicy) -> tuple[DatasetPolicy, ...]:
    """The shipped policies before this exact one, newest first; none for a descriptor the ledger does not record.

    A lookup by digest: the installed policy reads every earlier entry that is not ``unread``, and an earlier entry
    the ones before it. A fixture, a partial writer schema or a changed policy not yet recorded shares a version
    label at most, and reads nothing it did not ship after.
    """
    installed, earlier = _recorded(policy.dataset)
    digest = descriptor_digest(policy)
    found = len(earlier) if digest == installed else next(
        (at for at, entry in enumerate(earlier) if entry.digest == digest), None)
    return () if found is None else tuple(e.policy for e in reversed(earlier[:found]) if e.unread is None)


def earlier_policy(dataset: str, policy_version: str) -> DatasetPolicy:
    """The recorded earlier policy of ``dataset`` published under ``policy_version``, ``unread`` or not."""
    for entry in _recorded(dataset)[1]:
        if entry.policy.policy_version == policy_version:
            return entry.policy
    raise KeyError(f"{dataset} recorded no earlier policy {policy_version} ({LEDGER})")


def shipped(dataset: str, migrations: Mapping | None = None) -> list[str]:
    """The digests a generation of ``dataset`` may still state, in order: its read earlier entries, the installed."""
    entry = (ledger() if migrations is None else migrations).get(dataset)
    if entry is None:
        return []
    return [e.digest for e in _entries(entry) if e.unread is None] + [entry["installed"]]


def replay_reason(policy, migrations: Mapping | None = None) -> str | None:
    """Why rows selected under this exact policy are rebuilt rather than carried; ``None`` when they carry."""
    earlier = _recorded(policy.dataset)[1] if migrations is None else _entries(migrations.get(policy.dataset, {}))
    digest = descriptor_digest(policy)
    return next((e.replay for e in earlier if e.digest == digest), None)


def unreadable_shipped_policies(installed: Mapping | None = None, migrations: Mapping | None = None) -> list[str]:
    """Each installed policy the ledger does not record as current, and each malformed earlier entry.

    Every recorded earlier entry is read unless it says why not. What can still be wrong: the installed file
    changed without being recorded (the 2026-11-09 failure, ss-aj7t: a nightly refused its own prior), two recorded
    descriptors share a version label (receipts name their policy by label), or a marker states no reason, or a
    replay-required entry has no code that replays it.
    """
    from spicy_regs.etl_policy_registry import installed_policies

    installed = installed_policies() if installed is None else installed
    migrations = ledger() if migrations is None else migrations
    problems = []
    # A table whose receipts are retired is checked too: the change that marks it may promote columns as well, and
    # its last night on receipts reads its prior under the new policy.
    for name, policy in sorted(installed.items()):
        entry = migrations.get(name, {})
        current = descriptor_digest(policy)
        if entry.get("installed") != current:
            problems.append(f"{name}: installed {policy.policy_version} ({current}) is not recorded as current in "
                            f"{LEDGER}; record it with scripts/update_etl_policy_declarations.py, which keeps the "
                            "previous one as an earlier entry")
            continue
        if any(declared.get("dataset") != name or set(declared) - MARKERS != set(policy.descriptor())
               or len(MARKERS & set(declared)) > 1 for declared in entry.get("earlier", ())):
            problems.append(f"{name}: an earlier entry is not a descriptor of this dataset with at most one marker")
            continue
        earlier = _entries(entry)
        versions = [e.policy.policy_version for e in earlier] + [policy.policy_version]
        if len(set(versions)) != len(versions):
            problems.append(f"{name}: two recorded descriptors share a policy_version: {versions}")
        for e in earlier:
            for marker, reason in ((REPLAY_REQUIRED, e.replay), (UNREAD, e.unread)):
                if reason is not None and not str(reason).strip():
                    problems.append(f"{name}: {marker} {e.digest} states no reason")
            if e.replay is not None and str(e.replay).strip() and not _registered(name):
                problems.append(f"{name}: replay-required in {LEDGER} but no code replays it "
                                "(policy_migrations.REPLAYS)")
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
    from spicy_regs.subject_catalog import descriptor_of

    index = publication.current_index(base_url)
    problems = []
    for family, entry in sorted(index["families"].items()):
        _, root = publication.load_family_root(base_url, entry)
        for declared in root.get("spec", {}).get("etlReceipts", {}).get("policies", []):
            policy = DatasetPolicy.from_descriptor(descriptor_of(declared))
            if descriptor_digest(policy) not in shipped(policy.dataset):
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
