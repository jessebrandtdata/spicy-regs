"""A table whose receipts are retired, through every reader that took "installed policy" to mean "has receipts".

The marker (``subject_catalog.RECEIPTS_RETIRED``) sits beside the table's descriptor in its installed policy file, so
retiring moves no policy version and no ledger entry. Nothing is marked in the installed files yet; these tests mark
a table by patching the one function every reader asks (``subject_catalog.retired_receipt_datasets``).
"""

import copy
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

from spicy_regs import subject_catalog
from spicy_regs.policy_migrations import descriptor_digest, ledger, unreadable_shipped_policies

REGISTER = Path(__file__).parent / "fixtures/federal_register_switch/expected/night-1.parquet"


@pytest.fixture
def retire(monkeypatch):
    """Mark tables retired for one test, as their installed files would."""
    def mark(*names):
        monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: frozenset(names))
    return mark


def test_nothing_is_retired_in_the_installed_files_yet():
    subject_catalog.retired_receipt_datasets.cache_clear()
    assert subject_catalog.retired_receipt_datasets() == frozenset()


def test_the_marker_is_a_tables_and_never_part_of_its_policy():
    declared = {**subject_catalog.descriptors()["federal_register"], subject_catalog.RECEIPTS_RETIRED: True}
    assert subject_catalog.is_receipts_retired(declared)
    # Stripped before digesting: the policy, its version and its ledger entry are what they were.
    assert subject_catalog.descriptor_of(declared) == subject_catalog.descriptors()["federal_register"]
    assert descriptor_digest(subject_catalog.policies()["federal_register"]) == ledger()["federal_register"]["installed"]
    shared_log = subject_catalog.descriptors()["congress_acquisition"]
    for refused in ({**declared, subject_catalog.RECEIPTS_RETIRED: "yes"},
                    {**shared_log, subject_catalog.RECEIPTS_RETIRED: True},
                    {**declared, subject_catalog.SHARED_LOG: True}):
        with pytest.raises(ValueError):
            subject_catalog.descriptor_of(refused)


def test_admission_needs_no_receipts_for_a_retired_table_and_refuses_them(retire):
    from spicy_regs.etl_policy_registry import require_registered_receipts

    tables = {"federal_register.parquet": {}}
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        require_registered_receipts(tables, None)
    retire("federal_register")
    require_registered_receipts(tables, None)
    stated = {"policies": [subject_catalog.descriptors()["federal_register"]]}
    with pytest.raises(ValueError, match=r"Receipts are retired for \['federal_register'\]"):
        require_registered_receipts(tables, stated)
    # Every other registered table still needs its receipts.
    with pytest.raises(ValueError, match="Registered datasets require ETL receipts"):
        require_registered_receipts({**tables, "fr_docket_links.parquet": {}}, None)


def test_the_ledger_guard_leaves_a_retired_tables_entry_as_the_record_of_its_receipts(retire):
    from spicy_regs.etl_policy_registry import installed_policies

    installed = installed_policies()
    current = installed["federal_register"]
    installed["federal_register"] = replace(current, subject_schema=current.subject_schema.append(
        pa.field("added_later", pa.string())), policy_version="regulations-native-v9")
    migrations = copy.deepcopy(dict(ledger()))
    assert [p.split(":")[0] for p in unreadable_shipped_policies(installed, migrations)] == ["federal_register"]
    retire("federal_register")
    assert unreadable_shipped_policies(installed, migrations) == []


def test_the_generator_keeps_the_marker_its_installed_file_carries(retire):
    """``scripts/update_etl_policy_declarations.py`` regenerates every declaration; the marker survives it."""
    from scripts.update_etl_policy_declarations import declarations

    retire("federal_register")
    declared = declarations(frozenset({"federal_register", "fr_docket_links"}))
    assert declared["federal_register"][subject_catalog.RECEIPTS_RETIRED] is True
    assert subject_catalog.RECEIPTS_RETIRED not in declared["fr_docket_links"]
    assert subject_catalog.descriptor_of(declared["federal_register"]) == subject_catalog.descriptors()["federal_register"]
