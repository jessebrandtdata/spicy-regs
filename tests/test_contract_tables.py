"""Hermetic tests for the contract-hosted tables.

No network. These cover the seam this repository actually owns — the
conversion from a contract's row shape to a published Parquet table — and
deliberately do not re-assert any rule. Whether ``stage`` is read correctly
from an action, or a version code is the right slug, is established in
spicy-docs beside the code that decides it; re-asserting it here would be a
second copy of the same claim that can only drift.

What is owned here: every hosted table publishes an all-VARCHAR schema in the
contract's column order, and the merge prefers the fresh row on a repeated
identity. Both are proved for every contract the wheel ships, hosted here or
not yet, by iterating ``TABLE_CONTRACTS`` rather than by listing tables, so a
table added upstream is covered the moment the wheel is adopted instead of
when someone remembers to add a case.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS

from spicy_regs import data_dictionary as dd
from spicy_regs.govinfo_revisions import DATED_TABLES
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.table_merge import merge_contract_table

CONTRACT_NAMES = sorted(TABLE_CONTRACTS)

#: A contract here leaves the set when its owning rollup hosts it.
UNHOSTED_CONTRACTS = frozenset()

#: Contracts this repository publishes through its own ``RECORD_TYPES`` (the
#: regulatory base tables), not through ``CONTRACT_TABLES``. SpicyDocs 0.34.1
#: contracts them so DocSpec can admit them by reference.
RECORD_TYPE_CONTRACTS = frozenset(RECORD_TYPES)


def _no_download(remote_key: str, local_path: Path) -> bool:
    """Stand-in for R2: there is no prior table, and nothing may reach the network."""
    return False


def _row(contract, **overrides) -> dict:
    """A full synthetic row with valid native identity dates and source-schema context."""
    row = {column: None for column in contract.columns}
    for index, column in enumerate(contract.identity):
        row[column] = f"id{index}"
    if contract.name in DATED_TABLES and "last_modified" in contract.identity:
        row["last_modified"] = "2026-09-18T12:00:00Z"
    if contract.name == "bill_publisher_summaries":
        row["source_schema_version"] = "3.0.0"
    row.update(overrides)
    return row


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_published_schema_is_the_contract_columns_as_varchar(tmp_path, name):
    contract = TABLE_CONTRACTS[name]
    out = merge_contract_table(tmp_path, name, [_row(contract)], download_prior=_no_download)

    assert out.name == f"{name}.parquet"
    schema = pq.read_table(out).schema
    assert list(zip(schema.names, schema.types)) == [(c, pa.string()) for c in contract.columns]


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_merge_prefers_the_fresh_row_on_a_repeated_identity(tmp_path, name):
    contract = TABLE_CONTRACTS[name]
    # A column that is neither identity nor the version column, so the
    # preference is visible without disturbing either.
    spare = next(
        (c for c in contract.columns if c not in contract.identity and c != contract.version_column),
        None,
    )
    if spare is None:  # pragma: no cover - every contract has one today
        pytest.skip(f"{name} is all identity")

    first = merge_contract_table(tmp_path, name, [_row(contract, **{spare: "prior"})], download_prior=_no_download)
    # The published table becomes the prior for the next run.
    first.rename(tmp_path / f"_{name}_prior.parquet")

    out = merge_contract_table(tmp_path, name, [_row(contract, **{spare: "fresh"})], download_prior=_no_download)
    rows = pq.read_table(out).to_pylist()
    assert len(rows) == 1, f"{name}: a repeated identity must collapse to one row"
    assert rows[0][spare] == "fresh"


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_a_row_missing_its_identity_is_dropped_not_published(tmp_path, name):
    """A null identity part cannot be keyed, so it must not reach the table."""
    contract = TABLE_CONTRACTS[name]
    good = _row(contract)
    bad = _row(contract)
    bad[contract.identity[0]] = None

    out = merge_contract_table(tmp_path, name, [good, bad], download_prior=_no_download)
    assert len(pq.read_table(out).to_pylist()) == 1


def test_every_hosted_table_is_registered_everywhere():
    """A hosted table is one of the wheel's contracts, and is published, queryable and described.

    The wheel ships more contracts than ``CONTRACT_TABLES`` hosts; the
    difference is the regulatory base tables this repository publishes through
    its own record types (``RECORD_TYPE_CONTRACTS``) plus ``UNHOSTED_CONTRACTS``,
    stated so a new upstream contract is a decision here rather than a silent
    omission from the hosted surface. The three sets partition the registry by
    name, so every contract the adopted wheel ships is accounted for
    individually rather than by a count.

    """
    from spicy_regs import mcp_server

    hosted = set(dd.CONTRACT_TABLES)
    assert {"law_sections", "bill_committee_activities", "cbo_feed_items", "gao_decisions"} <= hosted
    assert len(dd.CONTRACT_TABLES) == len(hosted), "hosted declarations must be unique"
    assert hosted <= set(TABLE_CONTRACTS), "every hosted table must be a wheel contract"
    assert hosted.isdisjoint(RECORD_TYPE_CONTRACTS) and UNHOSTED_CONTRACTS.isdisjoint(RECORD_TYPE_CONTRACTS)
    assert set(TABLE_CONTRACTS) - hosted == UNHOSTED_CONTRACTS | RECORD_TYPE_CONTRACTS, (
        "a wheel contract is hosted, published as a record type, or named as not yet"
    )
    assert UNHOSTED_CONTRACTS.isdisjoint(dd.TABLES), "an unhosted contract must not be half-listed"
    assert hosted - set(dd.subject_policies()[name].dataset for name in hosted if dd.subject_policies()[name].receipt_only) <= set(dd.TABLES)
    subjects = {name for name in hosted if not dd.subject_policies()[name].receipt_only}
    assert subjects <= set(mcp_server.TABLES)
    assert subjects <= set(dd.load_descriptions())


#: Columns a record type's contract appends that this host does not write yet, by table, in contract order. SpicyDocs
#: 0.52.0 appends ``comments.subtype`` and ``comments.duplicate_comments`` (its "Comments carry the agency's submitter
#: class and campaign count"); this branch writes them for real, so no contract column is pending.
PENDING_RECORD_COLUMNS: dict[str, tuple[str, ...]] = {}


@pytest.mark.parametrize("name", sorted(RECORD_TYPE_CONTRACTS))
def test_record_type_declares_the_same_table_as_its_contract(name):
    """A record type and the wheel's contract for it name the same columns, in order, and the same identity.

    A pending column is one the contract appends last, never one the record type skips mid-table.
    """
    contract, record_type = TABLE_CONTRACTS[name], RECORD_TYPES[name]
    pending = PENDING_RECORD_COLUMNS.get(name, ())
    assert contract.columns[len(contract.columns) - len(pending):] == pending
    assert tuple(record_type.schema) == contract.columns[: len(contract.columns) - len(pending)]
    assert (record_type.dedup_key,) == contract.identity


#: The ``congress_bills`` prefix other repositories pin by digest through
#: ``catalog.json``, stated here because the list writer that first published
#: it was retired by plan A1 (decision 31).
CONGRESS_BILLS_FROZEN_PREFIX = (
    "bill_id",
    "congress",
    "bill_type",
    "bill_number",
    "title",
    "origin_chamber",
    "latest_action_date",
    "latest_action_text",
    "update_date",
    "url",
)


def test_congress_bills_keeps_its_frozen_prefix():
    """Other repositories pin these ten columns by digest; the contract and the dictionary append only."""
    contract = TABLE_CONTRACTS["congress_bills"]
    assert contract.columns[:10] == CONGRESS_BILLS_FROZEN_PREFIX
    assert contract.identity == ("bill_id",)
    assert len(contract.columns) > 10, "the family appends columns; it does not replace them"
    dictionary = [column for column, _ in dd.expected_schemas()["congress_bills"]]
    assert dictionary == dd.subject_policies()["congress_bills"].subject_schema.names
    from spicy_regs.congress_subjects import map_record
    mapped = map_record("congress_bills", {"url_source": "test-route", "cosponsors_outcome": "unread"})
    assert "url_source" not in dictionary and "cosponsors_outcome" not in dictionary
    assert mapped.source_fields["url_source"] == "test-route" and mapped.source_fields["cosponsors_outcome"] == "unread"
