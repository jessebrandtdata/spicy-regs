"""Installed subject/receipt declarations without build-time imports in serving."""
from functools import lru_cache
from importlib.resources import files
import json

#: The one key a declaration may carry beside its ``DatasetPolicy.descriptor()``: ``"shared_log": true`` on a
#: receipt-only dataset that every family reading one source writes for itself (the acquisition and file-state
#: logs). Such a log has no owning family. Each family's rows are its own run history, stored in its own receipt
#: member and read back only through one of that family's subject datasets, so no publication index entry lists it
#: and nothing selects it by name. A retry checkpoint has the same policy shape and must not carry the marker: one
#: family owns a checkpoint, and its readers find it by name.
SHARED_LOG = "shared_log"
#: The key a table's installed declaration carries, beside its descriptor, once the table publishes no per-row ETL
#: receipts (ss-vse3): ``"receipts_retired": true``. The file stays the table's schema declaration, so retiring moves
#: no policy version and no ledger entry. The marker is the one owner of retirement: admission refuses receipts for
#: the table, publication accepts the first generation without them (and only for marked tables), and every reader
#: that would look for its receipts reads the published table instead. One-way: receipts never return.
RECEIPTS_RETIRED = "receipts_retired"
#: Every key a declaration may carry beside its descriptor; none is part of the policy a generation states.
MARKERS = frozenset({SHARED_LOG, RECEIPTS_RETIRED})


def is_shared_log(declared) -> bool:
    """Whether one installed declaration is a shared log; the marker on anything with a subject table is refused."""
    if SHARED_LOG not in declared:
        return False
    if declared[SHARED_LOG] is not True or declared["receipt_only"] is not True:
        raise ValueError(f"Only a receipt-only dataset can be declared a shared log: {declared['dataset']}")
    return True


def is_receipts_retired(declared) -> bool:
    """Whether one installed declaration's table no longer has receipts; the marker anywhere else is refused."""
    if RECEIPTS_RETIRED not in declared:
        return False
    if declared[RECEIPTS_RETIRED] is not True or declared["receipt_only"] is not False or SHARED_LOG in declared:
        raise ValueError(f"Only a table's declaration can retire its receipts: {declared['dataset']}")
    return True


def descriptor_of(declared) -> dict:
    """The policy a declaration states, its markers checked and left out: what generations and receipts carry."""
    is_shared_log(declared)
    is_receipts_retired(declared)
    return {key: value for key, value in declared.items() if key not in MARKERS}


@lru_cache(maxsize=1)
def _declarations():
    root = files("spicy_regs").joinpath("etl_policies")
    return {path.name.removesuffix(".json"): json.loads(path.read_text())
            for path in root.iterdir() if path.name.endswith(".json")}


@lru_cache(maxsize=1)
def descriptors():
    return {name: descriptor_of(declared) for name, declared in _declarations().items()}


@lru_cache(maxsize=1)
def shared_receipt_logs() -> frozenset[str]:
    """The receipt-only datasets several families write and none owns, from their declarations' marker."""
    return frozenset(name for name, declared in _declarations().items() if is_shared_log(declared))


@lru_cache(maxsize=1)
def retired_receipt_datasets() -> frozenset[str]:
    """The tables whose receipts are retired, from their declarations' marker (:data:`RECEIPTS_RETIRED`)."""
    return frozenset(name for name, declared in _declarations().items() if is_receipts_retired(declared))


@lru_cache(maxsize=1)
def policies():
    from .etl_policy_registry import installed_policies
    return installed_policies()


def subject_tables(declared):
    registry = descriptors()
    return tuple(name for name in dict.fromkeys((*declared, *sorted(registry)))
                 if name not in registry or not registry[name]["receipt_only"])


def receipt_datasets():
    return tuple(name for name, policy in descriptors().items() if policy["receipt_only"])
