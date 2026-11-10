"""Explicit installed dataset policies; family tasks own the descriptor files.

Each ``etl_policies/*.json`` is one DatasetPolicy.descriptor(), plus the
shared-log marker (``subject_catalog.SHARED_LOG``) on a log no family owns, or
the receipts-retired marker (``subject_catalog.RECEIPTS_RETIRED``) on a table
that no longer has receipts. Registering a policy makes receipt admission
mandatory for new builds and publications of that dataset until its receipts
are retired; from then on they are refused. Historical generation reads
continue to use their pinned policy.
"""
from importlib.resources import files
from collections.abc import Mapping
import json

from spicy_regs.etl_receipts import DatasetPolicy
from spicy_regs.subject_catalog import descriptor_of


def installed_policies() -> dict[str, DatasetPolicy]:
    root = files("spicy_regs").joinpath("etl_policies")
    if not root.is_dir():
        return {}
    policies = {}
    for path in sorted(root.iterdir(), key=lambda p: p.name):
        if path.name.endswith(".json"):
            # The markers are not part of the policy a generation states, so receipts and manifests do not carry them.
            policy = DatasetPolicy.from_descriptor(descriptor_of(json.loads(path.read_text())))
            if path.name != policy.dataset + ".json" or policy.dataset in policies:
                raise ValueError("Installed field policy filename or identity differs")
            policies[policy.dataset] = policy
    return policies


def require_registered_receipts(tables, receipt_spec: Mapping | None) -> None:
    """A new generation states exactly the installed policy of each registered dataset it holds or claims.

    A table whose receipts are retired needs none and may state none: the marker makes the change one-way on every
    path that builds or publishes a generation, whether or not that path knows the table changed.
    """
    from spicy_regs.subject_catalog import retired_receipt_datasets

    descriptors = receipt_spec["policies"] if receipt_spec is not None else []
    declared = {p["dataset"]: p for p in descriptors}
    if len(declared) != len(descriptors):
        raise ValueError("Receipt policies contain duplicate datasets")
    retired = retired_receipt_datasets()
    if reclaimed := sorted(retired & declared.keys()):
        raise ValueError(f"Receipts are retired for {reclaimed}; a generation may not state receipts for them again")
    required = {name: p for name, p in installed_policies().items()
                if name not in retired and (name + ".parquet" in tables or name in declared)}
    if not required:
        return
    if receipt_spec is None:
        raise ValueError(f"Registered datasets require ETL receipts: {sorted(required)}")
    for name, policy in required.items():
        if declared.get(name) != policy.descriptor():
            raise ValueError(f"New generation differs from installed field policy: {name}")
