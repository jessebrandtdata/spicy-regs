"""read_receipt_fields at the request boundary: every refusal, receipt outcome and field state, read from receipts
the shared writer wrote for rows each family's own mapper shaped."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server as server
from spicy_regs import receipt_field_declarations as declarations
from spicy_regs import receipt_lookup as lookup
from spicy_regs.etl_policy_registry import installed_policies
from spicy_regs.etl_receipts import subject_identity
from spicy_regs.relationship_views.core import DETAIL_STATES, install_arrays
from tests.receipt_lookup_fixtures import GENERATION, index_of, one_family, published, serving, written
from tests.test_mcp_server import _tool_data

POLICIES = installed_policies()


def call(table: str, keys: list, fields: list[str], **extra) -> dict:
    return _tool_data(server.build_server(), lookup.TOOL, {"table": table, "keys": keys, "fields": fields, **extra})


def refused(table: str, keys: list, fields: list[str], **extra) -> str:
    with pytest.raises(ToolError) as error:
        call(table, keys, fields, **extra)
    return str(error.value)


def fcc(identifier: str, **raw) -> dict:
    """An fcc_filings row as the government writer hands it on: typed subject values beside its producer row."""
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    row = {**dict.fromkeys(LEGACY_COLUMNS["fcc_filings"]), "id_submission": identifier,
           "filing_url": f"https://www.fcc.gov/ecfs/filing/{identifier}", **raw}
    return {**map_subject("fcc_filings", row), "raw_record": row}


def congress(table: str, **raw) -> dict:
    """A Congress row as its writer hands it on: the mapper's subject beside the shaped source row."""
    from spicy_regs.congress_subjects import INPUT_COLUMNS, map_record

    mapped = map_record(table, {**dict.fromkeys(INPUT_COLUMNS[table]), **raw})
    assert mapped.subject is not None
    return {**mapped.subject, "source_fields": mapped.source_fields, "entry_kind": "row"}


def carried(table: str) -> dict[str, str | None]:
    fields = lookup.carried_fields(table)
    assert fields is not None
    return fields


def member(con, table: str) -> lookup.Member:
    selected = lookup.selected_member(server._connection_index(con.cursor()), None, server.R2_BASE_URL, table)
    assert selected is not None
    return selected


def without(table: pa.Table, record_ids: set[str], *, keep: bool = False) -> pa.Table:
    """``table`` without the receipts of ``record_ids``, or with only them."""
    return table.filter(pa.array([(record_id in record_ids) == keep for record_id in table["record_id"].to_pylist()]))


def states(reply: dict, position: int = 0) -> dict[str, str]:
    return {name: field["state"] for name, field in reply["keys"][position]["fields"].items()}


# The reply: shared facts once, decoded values, request order.

def test_a_found_receipt_gives_decoded_plain_values_in_request_order(tmp_path, monkeypatch):
    nested = {"amount": Decimal("1.50"), "when": date(2026, 1, 2), "parts": [1, None, {"deep": True}]}
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": [
        fcc("100", native_fields_json=nested), fcc("0427547924954", text_data="held")]}, family="fcc-filings")
    reply = call("fcc_filings", [{"id_submission": "0427547924954"}, {"id_submission": "100"}],
                 ["filing_url", "native_fields_json", "text_data"])
    family = server._connection_index(con.cursor())["families"]["fcc-filings"]
    assert reply["table"] == "fcc_filings" and reply["identity_fields"] == {"id_submission": "VARCHAR"}
    assert reply["receipts"] == {"family": "fcc-filings", "generation_id": GENERATION,
                                 "artifact_digest": family["artifactDigest"], "policy_version": POLICIES["fcc_filings"].policy_version}
    assert [entry["key"] for entry in reply["keys"]] == [{"id_submission": "0427547924954"}, {"id_submission": "100"}]
    leading_zero, hundred = (entry["fields"] for entry in reply["keys"])
    assert leading_zero["filing_url"] == {"state": "stated", "value": "https://www.fcc.gov/ecfs/filing/0427547924954"}
    assert leading_zero["text_data"] == {"state": "stated", "value": "held"}
    # A nested value comes back as plain JSON: no type tags, an exact decimal as its text, a date as ISO text.
    assert hundred["native_fields_json"] == {
        "state": "stated", "value": {"amount": "1.50", "when": "2026-01-02", "parts": [1, None, {"deep": True}]}}
    assert '["str"' not in json.dumps(reply, separators=(",", ":"))
    # Each shared fact is stated once: a field's meaning and place, and only the words this reply uses.
    assert reply["fields"]["filing_url"] == {
        "kept_in": "raw_record",
        "meaning": "Canonical fcc.gov URL for the filing (`https://www.fcc.gov/ecfs/filing/<id_submission>`)."}
    assert leading_zero["native_fields_json"] == {"state": "null_unmarked"}
    assert set(reply["receipt_meaning"]) == {"found"} and set(reply["state_meaning"]) == {"stated", "null_unmarked"}


def test_the_whole_mapping_is_a_field_only_where_the_policy_declares_it(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]})
    reply = call("fcc_filings", [{"id_submission": "100"}], ["raw_record"])
    assert reply["keys"][0]["fields"]["raw_record"]["value"]["filing_url"] == "https://www.fcc.gov/ecfs/filing/100"
    assert reply["fields"]["raw_record"] == {"kept_in": None, "meaning": None}


# Each family's own mapper output is read where the declaration says its fields are.

def _government():
    return "fcc_filings", fcc("100", total_page_count="2"), {"filing_url": "https://www.fcc.gov/ecfs/filing/100",
                                                             "total_page_count": "2"}


def _congress():
    return ("bill_actions", congress("bill_actions", bill_id="119-hr-1", action_index="3", source_system_code="9"),
            {"source_system_code": "9", "action_index": "3", "entry_kind": "row"})


def _legislative():
    from spicy_regs.legislative_documents import field_registry
    from spicy_regs.legislative_receipts import mapped_record

    spec = field_registry()["hearing_transcripts"]
    raw = {**dict.fromkeys(field["name"] for field in spec["fields"]), "package_id": "CHRG-119hhrg1", "congress": "119", "last_modified": "2026-09-18T12:00:00Z"}
    return "hearing_transcripts", mapped_record("hearing_transcripts", raw), {"congress": "119"}


def _regulations():
    from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
    from spicy_regs.transforms.regulations_shape import shape_record

    raw = {**dict.fromkeys(name for name, _ in SOURCE_COLUMNS["federal_register"]), "document_number": "2026-12696",
           "publication_date": "2026-06-25", "html_url": "https://www.federalregister.gov/d/2026-12696",
           "start_page": "100"}
    return ("federal_register", {**shape_record("federal_register", raw), "input_metadata": {}},
            {"html_url": "https://www.federalregister.gov/d/2026-12696", "start_page": "100"})


def _fec_identity():
    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY, normalize_record

    raw = {**dict.fromkeys(REGISTRY["fec_candidate_history"]["input_fields"]), "candidate_id": "H0XX00001",
           "cycle": "2024", "election_year": "2026"}
    return "fec_candidate_history", normalize_record("fec_candidate_history", raw), {"election_year": "2026"}


def _fec_subject():
    from spicy_regs.transforms.fec_subject_receipts import mapped_record

    row = {"record_id": "r1", "mapping_reason_json": '["source_status"]'}
    return ("fec_account_transfers", mapped_record("fec_account_transfers", row, POLICIES["fec_account_transfers"]),
            {"mapping_reason_json": '["source_status"]'})


def _scorecards():
    from spicy_regs.scorecards.subject_shapes import SOURCE_COLUMNS, map_source_row

    raw = {**dict.fromkeys(SOURCE_COLUMNS["scorecard_member_ratings"]), "scorecard_id": "s", "metric_id": "m",
           "publisher_member_key": "k", "value_number": "87.50", "source_url": "https://publisher.test/score"}
    return ("scorecard_member_ratings", map_source_row("scorecard_member_ratings", raw),
            {"value_number": "87.50"})


@pytest.mark.parametrize("family", [_government, _congress, _legislative, _regulations, _fec_identity, _fec_subject,
                                    _scorecards])
def test_each_family_mapper_output_is_read_where_its_declaration_places_it(tmp_path, monkeypatch, family):
    table, record, expected = family()
    con = one_family(tmp_path, monkeypatch, {table: [record]})
    policy = POLICIES[table]
    key = {name: record[name] for name in policy.identity_fields}
    reply = call(table, [key], list(expected))
    assert reply["keys"] == [{"key": key, "receipt": "found", "fields": {
        name: {"state": "stated", "value": value} for name, value in expected.items()}}]
    assert reply["receipts"]["policy_version"] == policy.policy_version
    if family is _scorecards:
        assert con.execute('SELECT source_url FROM scorecard_member_ratings').fetchone() == ("https://publisher.test/score",)
        assert "source_url" not in carried(table)
    # The declaration offers exactly the policy's receipt fields and what its mappings are registered to hold.
    assert set(policy.receipt_fields) <= set(carried(table))
    assert {container for container in carried(table).values() if container} <= set(policy.receipt_fields)


# Refusal 1: an unknown table, or one whose family has no receipts in this connection.

def test_an_unknown_table_and_a_table_without_receipts_here_are_refused_naming_the_tables_that_have_them(
        tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]})
    unknown = refused("fcc_filing", [{"id_submission": "100"}], ["filing_url"])
    assert "'fcc_filing' is not a table with a row identity" in unknown
    assert "Tables whose receipts this connection holds: fcc_filings." in unknown
    unpublished = refused("congress_bills", [{"bill_id": "119-hr-1"}], ["url"])
    assert "congress_bills has no receipts in this connection (nothing here publishes them yet)" in unpublished
    assert "Tables whose receipts this connection holds: fcc_filings." in unpublished
    # Processing evidence has receipts and no row to key them by.
    assert "is not a table with a row identity" in refused("committee_report_reads", [{"package_id": "x"}], ["outcome"])


# Refusal 2: a field the table's receipts do not carry.

def test_a_field_the_receipts_do_not_carry_is_refused_listing_those_they_do(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]})
    message = refused("fcc_filings", [{"id_submission": "100"}], ["filing_url", "filing_link", "proceedings"])
    assert "fcc_filings receipts do not carry 'filing_link', 'proceedings'." in message
    assert "They carry: raw_record, id_submission, proceeding_names_json" in message and "filing_url" in message


def test_a_policy_that_keeps_no_whole_row_mapping_takes_its_fields_off_offer(tmp_path, monkeypatch):
    """A policy may stop keeping a row's fields as its mapper received them (a digest and a pointer in their
    place, say): what is on offer follows the installed policy on every call, with no rebuild of the bundled
    declaration, so nothing here assumes a receipt holds a whole original record."""
    from spicy_regs import subject_catalog

    assert carried("federal_register")["start_page"] == "raw_conversion_inputs"
    installed = subject_catalog.descriptors()
    slim = {**installed["federal_register"],
            "receipt_fields": [name for name in installed["federal_register"]["receipt_fields"]
                               if name != "raw_conversion_inputs"] + ["source_row_sha256"]}
    monkeypatch.setattr(lookup, "descriptors", lambda: {**installed, "federal_register": slim})
    table, record, _ = _regulations()
    one_family(tmp_path, monkeypatch, {table: [record]})
    key = [{"document_number": "2026-12696", "publication_date": "2026-06-25"}]
    message = refused(table, key, ["start_page"])
    assert "federal_register receipts do not carry 'start_page'" in message
    offered = message.split("They carry: ")[1]
    assert "html_url" in offered and "source_row_sha256" in offered and "document_number" not in offered
    # A field the policy still declares answers as before, and a newly declared one needs no other declaration.
    reply = call(table, key, ["html_url", "source_row_sha256"])
    assert states(reply) == {"html_url": "stated", "source_row_sha256": "null_unmarked"}
    described = _tool_data(server.build_server(), "describe_table", {"table": table})["receipt_fields"]["fields"]
    assert "source_row_sha256" in described and "start_page" not in described


def test_a_receipt_written_without_the_mapping_refuses_its_fields_instead_of_calling_them_null(tmp_path, monkeypatch):
    """The server's policy still declares the mapping; the publication was written under one that dropped it."""
    table, record, _ = _regulations()
    record = {name: value for name, value in record.items() if name != "raw_conversion_inputs"}
    one_family(tmp_path, monkeypatch, {table: [record]})
    key = [{"document_number": "2026-12696", "publication_date": "2026-06-25"}]
    message = refused(table, key, ["html_url", "start_page"])
    assert "federal_register receipts do not carry 'start_page' in this generation" in message
    assert "they hold no raw_conversion_inputs" in message
    offered = message.split("They carry: ")[1]
    assert "html_url" in offered and "start_page" not in offered and "raw_conversion_inputs" not in offered
    assert states(call(table, key, ["html_url"])) == {"html_url": "stated"}


# Refusal 3: a key that does not parse for the table's identity is refused, never answered as a miss.

@pytest.mark.parametrize(("table", "key", "problem"), [
    ("bill_actions", {"bill_id": "119-hr-1"}, "gives no action_index"),
    ("bill_actions", {"bill_id": "119-hr-1", "action_index": 3, "action": 1}, "names 'action'"),
    ("bill_actions", {"bill_id": 119, "action_index": 3}, "gives bill_id as int; it is text, so quote it"),
    ("bill_actions", {"bill_id": "119-hr-1", "action_index": "03"}, "gives action_index as '03'; it is a whole number"),
    ("bill_actions", {"bill_id": "119-hr-1", "action_index": 3.5}, "gives action_index as 3.5"),
    ("bill_actions", {"bill_id": "119-hr-1", "action_index": True}, "gives action_index as True"),
    ("bill_actions", {"bill_id": None, "action_index": 3}, "gives null for bill_id"),
])
def test_a_key_that_does_not_fit_the_identity_is_refused_naming_its_fields_and_types(
        tmp_path, monkeypatch, table, key, problem):
    one_family(tmp_path, monkeypatch, {"bill_actions": [_congress()[1]]})
    message = refused(table, [{"bill_id": "119-hr-1", "action_index": 3}, key], ["source_system_code"])
    assert f"Key 2 of 2 {problem}" in message
    assert "A key for bill_actions is an object giving exactly bill_id (text), action_index (integer)" in message


def test_every_installed_row_identity_has_supported_native_key_typing():
    """Typing a native key as text would digest to another record id."""
    import pyarrow as pa

    for table, policy in POLICIES.items():
        for name in policy.identity_fields:
            assert lookup._kind(policy.subject_schema.field(name).type) in ("text", "integer", "timestamp"), (table, name)
    with pytest.raises(ValueError, match="UTC timestamp row identities only"):
        lookup._kind(pa.date32())


def test_an_integer_outside_its_column_range_and_a_non_object_key_are_refused(tmp_path, monkeypatch):
    table, record, _ = _fec_identity()
    one_family(tmp_path, monkeypatch, {table: [record]})
    message = refused(table, [{"candidate_id": "H0XX00001", "cycle": 2 ** 40}], ["election_year"])
    assert "gives cycle as 1099511627776; it is a whole number (int32)" in message
    assert "candidate_id (text), cycle (integer)" in message
    assert "keys.0: Input should be a valid dictionary" in refused(table, ["H0XX00001"], ["election_year"])


def test_a_nullable_identity_field_takes_null_and_still_must_be_given(tmp_path, monkeypatch):
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    raw = {**dict.fromkeys(LEGACY_COLUMNS["sam_entities"]), "uei": "ABC123DEF456", "cage_code": "1AB23"}
    one_family(tmp_path, monkeypatch, {"sam_entities": [{**map_subject("sam_entities", raw), "raw_record": raw}]})
    reply = call("sam_entities", [{"uei": "ABC123DEF456", "entity_eft_indicator": None}], ["cage_code"])
    assert reply["keys"][0]["fields"] == {"cage_code": {"state": "stated", "value": "1AB23"}}
    message = refused("sam_entities", [{"uei": "ABC123DEF456"}], ["cage_code"])
    assert "gives no entity_eft_indicator" in message and "entity_eft_indicator (text, may be null)" in message


# Refusal 4: more than 100 keys, and an argument the tool does not take.

def test_more_than_a_hundred_keys_and_a_misspelled_argument_are_refused(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]})
    keys = [{"id_submission": str(number)} for number in range(101)]
    assert "keys holds 101 keys; read_receipt_fields reads at most 100 a call" in refused(
        "fcc_filings", keys, ["filing_url"])
    assert len(call("fcc_filings", keys[:100], ["filing_url"])["keys"]) == 100
    message = refused("fcc_filings", keys[:1], ["filing_url"], feilds=["filing_url"])
    assert "feilds is not an argument" in message and "read_receipt_fields takes fields, keys, table." in message
    with pytest.raises(ToolError, match="fields is required"):
        _tool_data(server.build_server(), lookup.TOOL, {"table": "fcc_filings", "keys": keys[:1]})
    assert "keys: List should have at least 1 item" in refused("fcc_filings", [], ["filing_url"])


# Refusal 5: receipts written under another identity definition.

def _under(tmp_path, monkeypatch, policies_and_rows) -> None:
    """Publish fcc_filings receipts written under each given policy in turn, as one member."""
    subjects, receipts = [], []
    for number, (policy, rows) in enumerate(policies_and_rows):
        subject, receipt = written(tmp_path / f"work-{number}", "fcc_filings", rows, policy=policy)
        subjects.append(pq.read_table(subject))
        receipts.append(pq.read_table(receipt))
    held = tmp_path / "subject.parquet"
    pq.write_table(pa.concat_tables(subjects, promote_options="default"), held)
    root = tmp_path / "bucket"
    serving(monkeypatch, root, index_of({"fcc-filings": published(
        root, "fcc-filings", {"fcc_filings": held}, pa.concat_tables(receipts))}))


def test_receipts_under_another_identity_definition_are_refused_before_any_key_is_called_missing(tmp_path, monkeypatch):
    installed = POLICIES["fcc_filings"]
    other = replace(installed, identity_fields=("id_submission", "submission_type"),
                    policy_version="government-sources/9")
    _under(tmp_path, monkeypatch, [(other, [fcc("100", submission_type="COMMENT")])])
    message = refused("fcc_filings", [{"id_submission": "100"}], ["filing_url"])
    assert "written under policy_version 'government-sources/9'" in message
    assert "whose row identity is id_submission (text), submission_type (text)" in message
    assert f"this server's policy ('{installed.policy_version}') identifies a row by id_submission (text)" in message


def test_record_ids_this_server_does_not_reproduce_are_refused_before_any_key_is_called_missing(tmp_path, monkeypatch):
    """Names and types agree and the ids still differ: the sampled receipt's stored id is recomputed from its own
    identity, which is the whole statement that this server finds this publication's receipts."""
    import hashlib

    def another_digest(table: pa.Table) -> pa.Table:
        ids = [hashlib.sha256(b"another digest:" + value.encode()).hexdigest() for value in table["record_id"].to_pylist()]
        return table.set_column(table.schema.get_field_index("record_id"), "record_id", pa.array(ids, pa.string()))

    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]}, edit=another_digest)
    message = refused("fcc_filings", [{"id_submission": "100"}], ["filing_url"])
    assert f"written under policy_version '{POLICIES['fcc_filings'].policy_version}' hold record ids this server does not reproduce" in message
    assert "id_submission (text)" in message and "Every key would miss" in message


def test_another_policy_version_with_the_same_identity_is_read_and_named(tmp_path, monkeypatch):
    installed = POLICIES["fcc_filings"]
    _under(tmp_path, monkeypatch, [(replace(installed, policy_version="government-sources/0"), [fcc("100")])])
    reply = call("fcc_filings", [{"id_submission": "100"}], ["filing_url"])
    assert reply["keys"][0]["receipt"] == "found" and reply["receipts"]["policy_version"] == "government-sources/0"


def test_every_version_a_member_holds_is_compared_and_one_differing_identity_refuses(tmp_path, monkeypatch):
    installed = POLICIES["fcc_filings"]
    earlier = replace(installed, policy_version="government-sources/0")
    _under(tmp_path, monkeypatch, [(installed, [fcc("100")]), (earlier, [fcc("200")])])
    reply = call("fcc_filings", [{"id_submission": "100"}, {"id_submission": "200"}], ["filing_url"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["found", "found"]
    assert reply["receipts"]["policy_version"] == ["government-sources/0", installed.policy_version]
    typed = replace(installed, identity_fields=("id_submission", "total_page_count"), policy_version="government-sources/8")
    _under(tmp_path / "second", monkeypatch, [(installed, [fcc("100")]), (typed, [fcc("200", total_page_count="4")])])
    message = refused("fcc_filings", [{"id_submission": "100"}], ["filing_url"])
    assert "'government-sources/8'" in message and "total_page_count (integer)" in message


# Refusal 6 and the scan it bounds: no member is ordered by record id.

def _descending(table: pa.Table) -> pa.Table:
    return table.sort_by([("record_id", "descending")])


def test_keys_are_found_wherever_their_receipts_lie_in_the_member(tmp_path, monkeypatch):
    rows = [fcc(str(number)) for number in range(100, 109)]
    one_family(tmp_path, monkeypatch, {"fcc_filings": rows}, row_group=2, edit=_descending)
    keys = [{"id_submission": str(number)} for number in (108, 100, 104, 999)]
    reply = call("fcc_filings", keys, ["filing_url"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["found", "found", "found", "not_in_table"]
    assert [entry["fields"]["filing_url"]["value"][-3:] for entry in reply["keys"][:3]] == ["108", "100", "104"]


def test_a_table_past_the_bound_is_refused_from_its_footer_alone(tmp_path, monkeypatch):
    rows = [fcc(str(number)) for number in range(100, 109)]
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 8)
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": rows}, row_group=2, edit=_descending)
    recording = _Recording(con)
    monkeypatch.setattr(server, "_get_connection", lambda: recording)
    message = refused("fcc_filings", [{"id_submission": "100"}], ["filing_url"])
    assert "read_receipt_fields is not available for fcc_filings: a lookup reads the record id of every receipt" in message
    assert "it has 9, over the 8-receipt bound" in message
    # The refusal is decided from the member's footer: no row of a member too large to scan is read to refuse it.
    assert [sql.split(" FROM ")[1].split("(")[0] for sql, _ in recording.statements
            if "read_parquet" in sql or "parquet_metadata" in sql] == ["parquet_metadata"]


def _add_selected_index(con):
    from pathlib import Path
    from spicy_regs.receipt_key_index import KEY
    from spicy_regs.receipt_key_index_writer import build_key_index

    snapshot = json.loads(con.execute("SELECT snapshot FROM _spicy_publication").fetchone()[0])
    entry = next(iter(snapshot["families"].values()))
    directory = Path(server.R2_BASE_URL) / entry["prefix"]
    entry["etlReceipts"]["keyIndex"] = build_key_index(
        directory / entry["etlReceipts"]["key"], directory / KEY, force=True)
    con.execute("UPDATE _spicy_publication SET snapshot=?", [json.dumps(snapshot)])
    return snapshot, entry


def test_selected_index_reads_large_table_without_receipt_id_scan(tmp_path, monkeypatch):
    rows = [fcc(str(number)) for number in range(100, 109)]
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": rows}, row_group=2, edit=_descending)
    keys = [{"id_submission": str(number)} for number in (108, 100, 108)]
    expected = call("fcc_filings", keys, ["filing_url"])
    _add_selected_index(con)
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 8)
    monkeypatch.setattr(lookup, "_accepted", lambda *a: pytest.fail("Indexed lookup scanned all receipt ids"))
    assert call("fcc_filings", keys, ["filing_url"]) == expected


def test_declared_index_wrong_receipt_pin_refuses_without_scan_fallback(tmp_path, monkeypatch):
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]})
    snapshot, entry = _add_selected_index(con)
    entry["etlReceipts"]["keyIndex"]["receiptSha256"] = "sha256:" + "f" * 64
    con.execute("UPDATE _spicy_publication SET snapshot=?", [json.dumps(snapshot)])
    monkeypatch.setattr(lookup, "_accepted", lambda *a: pytest.fail("Corrupt index fell back to a scan"))
    assert "Invalid or stale receipt key index descriptor" in refused(
        "fcc_filings", [{"id_submission": "100"}], ["filing_url"])


def test_indexed_lookup_retains_identity_version_refusal(tmp_path, monkeypatch):
    installed = POLICIES["fcc_filings"]
    other = replace(installed, identity_fields=("id_submission", "total_page_count"),
                    policy_version="government-sources/8")
    _under(tmp_path, monkeypatch, [(other, [fcc("100", total_page_count="4")])])
    con = server._get_connection()
    _add_selected_index(con)
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 0)
    message = refused("fcc_filings", [{"id_submission": "100"}], ["filing_url"])
    assert "Every key would miss" in message and "total_page_count (integer)" in message


def test_index_does_not_remove_missing_key_subject_search_bound(tmp_path, monkeypatch):
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc(str(n)) for n in range(100, 109)]})
    _add_selected_index(con)
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 8)
    assert "would search its 9 rows, over the 8-row bound" in refused(
        "fcc_filings", [{"id_submission": "999"}], ["filing_url"])


def test_two_tables_in_one_member_are_each_read_for_their_own_keys(tmp_path, monkeypatch):
    """Two datasets in one member, as gao-reports publishes: a row group shared at their boundary is read for both."""
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    def government(table, **values):
        raw = {**dict.fromkeys(LEGACY_COLUMNS[table]), **values}
        return {**map_subject(table, raw), "raw_record": raw}

    reports = [government("gao_reports", report_id=f"GAO-26-{number}", title=f"Report {number}") for number in range(7)]
    decisions = [government("gao_decisions", decision_number=f"B-{number}", url=f"https://www.gao.gov/products/b-{number}",
                            title=f"Decision {number}") for number in range(7)]
    con = one_family(tmp_path, monkeypatch, {"gao_reports": reports, "gao_decisions": decisions}, row_group=4)
    groups = lookup._footer(con.cursor(), member(con, "gao_decisions"))
    assert [group.datasets for group in groups] == [
        ("gao_reports", "gao_reports"), ("gao_decisions", "gao_reports"), ("gao_decisions", "gao_decisions"),
        ("gao_decisions", "gao_decisions")]
    reply = call("gao_decisions", [{"decision_number": f"B-{number}", "url": f"https://www.gao.gov/products/b-{number}"}
                                   for number in range(7)], ["title"])
    assert [entry["fields"]["title"]["value"] for entry in reply["keys"]] == [f"Decision {n}" for n in range(7)]
    reply = call("gao_reports", [{"report_id": f"GAO-26-{number}"} for number in range(7)], ["title"])
    assert [entry["fields"]["title"]["value"] for entry in reply["keys"]] == [f"Report {n}" for n in range(7)]


class _Recording:
    """A connection whose cursors record every statement with its parameters."""

    def __init__(self, con):
        self.con, self.statements = con, []

    def cursor(self):
        inner, statements = self.con.cursor(), self.statements

        class Cursor:
            def execute(self, sql, parameters=None):
                statements.append((sql, list(parameters or [])))
                return inner.execute(sql, parameters) if parameters is not None else inner.execute(sql)

            def __getattr__(self, name):
                return getattr(inner, name)

        return Cursor()


def test_the_receipts_page_shows_a_call_the_tool_answers(tmp_path, monkeypatch):
    """The one page that explains receipts leads with this tool; its example is run as written."""
    from spicy_regs import data_dictionary
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    (example,) = re.findall(r"```json\n(.*?)\n```", data_dictionary.RECEIPT_GUIDE.read_text(encoding="utf-8"), re.DOTALL)
    arguments = json.loads(example)
    assert set(arguments) == {"table", "keys", "fields"} and arguments["table"] == "crs_reports"
    link = "https://www.congress.gov/crs-product/R48641"
    raw = {**dict.fromkeys(LEGACY_COLUMNS["crs_reports"]), **arguments["keys"][0], "url": link}
    one_family(tmp_path, monkeypatch, {"crs_reports": [{**map_subject("crs_reports", raw), "raw_record": raw}]})
    reply = call(**arguments)
    assert reply["keys"] == [{"key": arguments["keys"][0], "receipt": "found",
                              "fields": {"url": {"state": "stated", "value": link}}}]


def test_crs_receipt_lookup_distinguishes_revisions_and_normalizes_timestamp_offsets(tmp_path, monkeypatch):
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    raw_rows = [{**dict.fromkeys(LEGACY_COLUMNS["crs_reports"]), "report_id": "R1",
                 "update_date": stamp, "url": f"https://example.test/revision-{index}"}
                for index, stamp in enumerate(("2026-07-17T22:38:55Z", "2026-07-17T23:38:55.123456Z"))]
    con = one_family(tmp_path, monkeypatch, {"crs_reports": [
        {**map_subject("crs_reports", raw), "raw_record": raw} for raw in raw_rows
    ]})
    keys = [{"report_id": "R1", "update_date": stamp} for stamp in (
        "2026-07-17T22:38:55Z", "2026-07-18T01:38:55.123456+02:00", "2026-07-19T00:00:00Z",
    )]
    reply = call("crs_reports", keys, ["url"])
    assert [item["receipt"] for item in reply["keys"]] == ["found", "found", "not_in_table"]
    assert [item["fields"]["url"]["value"] for item in reply["keys"][:2]] == [raw["url"] for raw in raw_rows]
    assert reply["keys"][1]["key"]["update_date"] == "2026-07-17T23:38:55.123456+00:00"
    _add_selected_index(con)
    assert call("crs_reports", keys, ["url"]) == reply
    for invalid in ("2026-07-17", "2026-07-17T22:38:55", "2026-02-30T22:38:55Z",
                    "2026-07-17T22:38:55.1234567Z", "2026-07-17T22:38:55+00:99", 123):
        message = refused("crs_reports", [{"report_id": "R1", "update_date": invalid}], ["url"])
        assert "use an ISO timestamp with an offset" in message


def test_one_scan_of_record_ids_then_only_the_row_groups_holding_a_key_are_read(tmp_path, monkeypatch):
    """What the tool asks DuckDB for: a single scan of record ids, never of receipts, to find its keys' rows, then
    one statement over the row ranges of the groups they are in."""
    rows = [fcc(str(number)) for number in range(100, 120)]
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": rows}, row_group=2, edit=_descending)
    recording = _Recording(con)
    monkeypatch.setattr(server, "_get_connection", lambda: recording)
    asked = ["104", "117"]
    reply = call("fcc_filings", [{"id_submission": identifier} for identifier in asked], ["filing_url"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["found", "found"]
    held = pq.read_table(member(con, "fcc_filings").location, columns=["record_id"])["record_id"].to_pylist()
    wanted = sorted(held.index(subject_identity(POLICIES["fcc_filings"], {"id_submission": identifier})[0]) // 2
                    for identifier in asked)
    locates = [parameters for sql, parameters in recording.statements if "SELECT record_id, file_row_number" in sql]
    [(fetch, parameters)] = [(sql, parameters) for sql, parameters in recording.statements if "processing_json" in sql]
    assert len(locates) == 1
    ranges = [tuple(parameters[1 + 2 * n:3 + 2 * n]) for n in range(fetch.count("file_row_number >= ?"))]
    assert ranges == [(2 * group, 2 * group + 2) for group in wanted] and len(set(wanted)) == 2
    # The scan reads the record ids of the dataset's rows and nothing else.
    for located in locates:
        assert located[1:3] == [0, 20] and "processing_json" not in [sql for sql, _ in recording.statements
                                                                      if "SELECT record_id, file_row_number" in sql][0]


# Refusal 7: a reply past the budget says how many keys fit.

def test_a_reply_past_the_budget_is_refused_with_the_number_of_keys_that_fit(tmp_path, monkeypatch):
    rows = [fcc(str(number), text_data="x" * 400) for number in range(100, 106)]
    one_family(tmp_path, monkeypatch, {"fcc_filings": rows})
    keys = [{"id_submission": str(number)} for number in range(100, 106)]
    monkeypatch.setattr(server, "REPLY_CHARS", 2_600)
    message = refused("fcc_filings", keys, ["text_data", "filing_url"])
    assert "over the 2,600-character reply limit; nothing is returned" in message
    [count] = re.findall(r"The first (\d+) of its 6 keys fit: ask for those, then the rest", message)
    fit = int(count)
    assert 1 <= fit < 6 and re.search(r"or for fewer fields \(text_data holds 8\d% of the values' characters\)", message)
    # The count is the most that fit: that many are answered within the limit, and one more is refused the same way.
    fits = call("fcc_filings", keys[:fit], ["text_data", "filing_url"])
    assert len(json.dumps(fits, separators=(",", ":"), ensure_ascii=False)) <= 2_600
    assert f"The first {fit} of its {fit + 1} keys fit" in refused("fcc_filings", keys[:fit + 1], ["text_data", "filing_url"])
    monkeypatch.setattr(server, "REPLY_CHARS", 1_000)
    assert "Not even the first key fits: ask for fewer fields" in refused("fcc_filings", keys, ["text_data"])


# The four receipt outcomes.

def test_several_accepted_receipts_for_one_key_are_counted_and_none_is_chosen(tmp_path, monkeypatch):
    def doubled(table: pa.Table) -> pa.Table:
        record_id = subject_identity(POLICIES["fcc_filings"], {"id_submission": "100"})[0]
        return pa.concat_tables([table, without(table, {record_id}, keep=True)])

    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100"), fcc("200")]}, edit=doubled)
    reply = call("fcc_filings", [{"id_submission": "100"}, {"id_submission": "200"}], ["filing_url"])
    assert reply["keys"][0] == {"key": {"id_submission": "100"}, "receipt": "ambiguous", "receipts_found": 2}
    assert reply["keys"][1]["receipt"] == "found" and set(reply["receipt_meaning"]) == {"found", "ambiguous"}


def test_a_key_the_table_holds_without_a_receipt_is_a_fault_apart_from_a_key_it_does_not_hold(tmp_path, monkeypatch):
    def dropped(table: pa.Table) -> pa.Table:
        return without(table, {subject_identity(POLICIES["fcc_filings"], {"id_submission": "200"})[0]})

    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100"), fcc("200")]}, edit=dropped)
    keys = [{"id_submission": "200"}, {"id_submission": "100"}, {"id_submission": "300"}, {"id_submission": "200"}]
    reply = call("fcc_filings", keys, ["filing_url"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["receipt_missing", "found", "not_in_table", "receipt_missing"]
    assert all("fields" not in entry for entry in reply["keys"] if entry["receipt"] != "found")
    assert "a fault in this publication" in reply["receipt_meaning"]["receipt_missing"]
    assert set(reply["receipt_meaning"]) == {"found", "receipt_missing", "not_in_table"}


def test_a_refused_attempt_with_the_key_is_not_an_accepted_receipt(tmp_path, monkeypatch):
    def refuse(table: pa.Table) -> pa.Table:
        return table.set_column(table.schema.get_field_index("outcome"), "outcome", pa.array(["refused"] * len(table)))

    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]}, edit=refuse)
    assert call("fcc_filings", [{"id_submission": "100"}], ["filing_url"])["keys"][0]["receipt"] == "receipt_missing"


def test_the_subject_search_for_a_key_without_a_receipt_is_bounded_and_only_run_for_such_keys(tmp_path, monkeypatch):
    rows = [fcc(str(number)) for number in range(100, 109)]
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 8)
    lost = {subject_identity(POLICIES["fcc_filings"], {"id_submission": "107"})[0]}
    # A table past the bound with receipts within it: the fault this search exists for, a row whose receipt is gone.
    con = one_family(tmp_path, monkeypatch, {"fcc_filings": rows}, edit=lambda table: without(table, lost))
    # Every key has a receipt: the 9-row table is not searched, so its size refuses nothing.
    assert len(call("fcc_filings", [{"id_submission": "100"}, {"id_submission": "108"}], ["filing_url"])["keys"]) == 2
    message = refused("fcc_filings", [{"id_submission": "100"}, {"id_submission": "nine"}], ["filing_url"])
    assert "No accepted receipt holds 1 of the keys asked for" in message
    assert "would search its 9 rows, over the 8-row bound" in message and '{"id_submission": "nine"}' in message
    con.close()


# The five field states.

def test_a_value_an_empty_value_and_a_null_no_marker_governs(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"fcc_filings": [
        fcc("100", text_data="", documents_json="[]", authors_json=' [ ] ', bureaus_json='["WCB"]', submission_type=None)]})
    reply = call("fcc_filings", [{"id_submission": "100"}],
                 ["text_data", "documents_json", "authors_json", "bureaus_json", "submission_type", "filing_url"])
    assert reply["keys"][0]["fields"] == {
        "text_data": {"state": "stated_empty", "value": ""},
        "documents_json": {"state": "stated_empty", "value": "[]"},
        "authors_json": {"state": "stated_empty", "value": " [ ] "},
        "bureaus_json": {"state": "stated", "value": '["WCB"]'},
        "submission_type": {"state": "null_unmarked"},
        "filing_url": {"state": "stated", "value": "https://www.fcc.gov/ecfs/filing/100"},
    }
    assert set(reply["state_meaning"]) == {"stated", "stated_empty", "null_unmarked"}
    assert "read_marker" not in reply["fields"]["submission_type"]


def test_gao_decided_date_has_no_marker_so_its_null_is_never_called_not_stated(tmp_path, monkeypatch):
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    raw = {**dict.fromkeys(LEGACY_COLUMNS["gao_decisions"]), "decision_number": "B-419046.2,B-419046.3",
           "url": "https://www.gao.gov/products/b-419046.2%2Cb-419046.3"}
    one_family(tmp_path, monkeypatch, {"gao_decisions": [{**map_subject("gao_decisions", raw), "raw_record": raw}]})
    reply = call("gao_decisions", [{"decision_number": raw["decision_number"], "url": raw["url"]}], ["decided_date"])
    assert reply["keys"][0]["fields"] == {"decided_date": {"state": "null_unmarked"}}


def _register(number: str, **raw) -> dict:
    from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
    from spicy_regs.transforms.regulations_shape import shape_record

    row = {**dict.fromkeys(name for name, _ in SOURCE_COLUMNS["federal_register"]), "document_number": number,
           "publication_date": "2026-06-25", **raw}
    return {**shape_record("federal_register", row), "input_metadata": {}}


def test_the_federal_register_marker_tells_unread_from_read_and_not_stated(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"federal_register": [
        _register("2026-00001"),
        _register("2026-00002", regulations_dot_gov_info_json="{}"),
        _register("2026-00003", regulations_dot_gov_info_json='{"docket_id":"EPA-HQ-OAR-2021-0317"}',
                  regulations_dot_gov_docket_id="EPA-HQ-OAR-2021-0317", action="Final rule.", corrections_json="[]"),
    ]})
    fields = ["regulations_dot_gov_info_json", "regulations_dot_gov_docket_id", "action", "corrections_json",
              "significant", "subtype"]
    reply = call("federal_register", [{"document_number": f"2026-0000{n}", "publication_date": "2026-06-25"}
                                      for n in (1, 2, 3)], fields)
    assert states(reply, 0) == {**dict.fromkeys(fields[:5], "unread"), "subtype": "null_unmarked"}
    assert states(reply, 1) == {"regulations_dot_gov_info_json": "stated_empty", **dict.fromkeys(fields[1:5], "not_stated"),
                                "subtype": "null_unmarked"}
    assert states(reply, 2) == {"regulations_dot_gov_info_json": "stated", "regulations_dot_gov_docket_id": "stated",
                                "action": "stated", "corrections_json": "stated_empty", "significant": "not_stated",
                                "subtype": "null_unmarked"}
    assert set(reply["state_meaning"]) == {"stated", "stated_empty", "unread", "not_stated", "null_unmarked"}
    assert "regulations_dot_gov_info_json is NULL on a row not read" in reply["fields"]["action"]["read_marker"]
    assert "read_marker" not in reply["fields"]["subtype"]


def _vote(vote_id: str, chamber: str, **raw) -> dict:
    return congress("roll_call_votes", vote_id=vote_id, chamber=chamber, **raw)


def test_a_roll_call_published_before_its_file_columns_reads_them_unread_for_its_own_chamber_only(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"roll_call_votes": [
        _vote("119-house-1-1", "house", legis_num="", clerk_body_element="rollcall-vote", party_totals_json="[]"),
        _vote("119-house-1-2", "house", legis_num="H R 1"),
        _vote("119-senate-1-3", "senate", vote_question_text="On Passage", majority_requirement="1/2", modify_date=""),
    ]})
    fields = ["legis_num", "clerk_body_element", "party_totals_json", "vote_title", "vote_desc"]
    reply = call("roll_call_votes", [{"vote_id": vote} for vote in ("119-house-1-1", "119-house-1-2", "119-senate-1-3")],
                 fields)
    # Read: the file states each column, '' at the least. A Senate column on a House row has no marker.
    assert states(reply, 0) == {"legis_num": "stated_empty", "clerk_body_element": "stated",
                                "party_totals_json": "stated_empty", "vote_title": "null_unmarked", "vote_desc": "null_unmarked"}
    # Published before two of the columns: they are unread, and the one it holds is still stated.
    assert states(reply, 1) == {"legis_num": "stated", "clerk_body_element": "unread", "party_totals_json": "unread",
                                "vote_title": "null_unmarked", "vote_desc": "null_unmarked"}
    # A Senate row: its own unread column is unread, and the House columns have no marker for it.
    assert states(reply, 2) == {"legis_num": "null_unmarked", "clerk_body_element": "null_unmarked",
                                "party_totals_json": "null_unmarked", "vote_title": "unread", "vote_desc": "null_unmarked"}


# detail_read: the tool and the field-state views answer the same for the same row and field.

MEETING_LISTS = {
    "bill_ids_json": '["119-hr-1"]', "hearing_jackets_json": '["60491"]',
    "committees_json": '[{"name":"Agriculture","systemCode":"hsag00","url":"https://api.congress.gov/c"}]',
    "document_urls_json": '["https://docs.house.gov/a.pdf"]',
    "witness_documents_json": '[{"documentType":"Statement","format":"PDF","url":"https://docs.house.gov/w.pdf"}]',
    "meeting_documents_json": '[{"description":"d","documentType":"Notice","format":"PDF","name":"n","url":"https://docs.house.gov/m.pdf"}]',
    "witnesses_json": '[{"name":"A Witness","position":"Director","organization":"Org"}]',
}
RIN_LIST = {"rin_occurrences_json": '[{"rin":"0648-AC64","matched_text":"0648-AC64","span_start":"1","span_end":"10"}]'}


@pytest.mark.parametrize(("table", "lists", "identity"), [
    ("committee_meetings", MEETING_LISTS, lambda n: {"congress": "119", "chamber": "house", "event_id": str(n)}),
    ("house_communications", RIN_LIST, lambda n: {"communication_id": f"119-ec-{n}", "congress": "119",
                                                  "communication_type": "ec", "number": str(n)}),
])
def test_the_tool_and_the_field_state_views_answer_alike_for_every_row_and_detail_field(
        tmp_path, monkeypatch, table, lists, identity):
    from spicy_regs.congress_receipts import write_congress_dataset
    from spicy_regs.congress_subjects import INPUT_COLUMNS
    from spicy_regs.relationship_views.congress import CONGRESS_RELATIONSHIPS
    from spicy_regs.relationship_views.regulatory import REGULATORY_RELATIONSHIPS

    rows, number = [], 0
    for read in ("true", "false", None):
        for values in (dict.fromkeys(lists), dict.fromkeys(lists, "[]"), lists):
            number += 1
            rows.append({**dict.fromkeys(INPUT_COLUMNS[table]), **identity(number), "detail_read": read, **values})
    source = tmp_path / "shaped.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in INPUT_COLUMNS[table]])), source)
    # The views read the legacy table; the tool reads the receipts the Congress writer makes from the same rows.
    views = [view for view in (*CONGRESS_RELATIONSHIPS, *REGULATORY_RELATIONSHIPS)
             if view.source_table == table and view.detail_read_column]
    assert {view.source_field for view in views} == set(lists)
    with duckdb.connect() as legacy:
        legacy.execute(f'CREATE TABLE "{table}" AS SELECT * FROM read_parquet(?)', [str(source)])
        install_arrays(legacy, [table], views)
        by_view = {}
        for view in views:
            cursor = legacy.execute(f'SELECT * FROM "{view.names[2]}"')
            names = [column[0] for column in cursor.description]
            by_view[view.source_field] = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    subject, receipts = write_congress_dataset(source, tmp_path / "bundle", dataset=table, generation_id=GENERATION)
    assert subject is not None
    root = tmp_path / "bucket"
    serving(monkeypatch, root, index_of({"family": published(root, "family", {table: subject}, pq.read_table(receipts))}))
    policy = POLICIES[table]
    reply = call(table, [{name: row[name] for name in policy.identity_fields} for row in rows], list(lists))
    seen = set()
    for row, entry in zip(rows, reply["keys"], strict=True):
        for field, held in by_view.items():
            [state] = [item["field_state"] for item in held
                       if all(item[key] == row[key] for key in views[0].source_keys)]
            assert entry["fields"][field]["state"] == state, (row["detail_read"], field, row[field])
            seen.add(state)
    assert seen == set(DETAIL_STATES)
    assert "detail_read is 'true'" in reply["fields"][next(iter(lists))]["read_marker"]


# Key canonicalisation: text is exact, numbers are numbers, and a composite key is every field.

def test_a_gao_decision_number_with_dots_and_commas_is_one_exact_text_beside_its_url(tmp_path, monkeypatch):
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS, map_subject

    number, url = "B-419046.2,B-419046.3", "https://www.gao.gov/products/b-419046.2%2Cb-419046.3"
    raw = {**dict.fromkeys(LEGACY_COLUMNS["gao_decisions"]), "decision_number": number, "url": url, "source": "gao_listing"}
    one_family(tmp_path, monkeypatch, {"gao_decisions": [{**map_subject("gao_decisions", raw), "raw_record": raw}]})
    reply = call("gao_decisions", [{"decision_number": number, "url": url},
                                   {"decision_number": number.lower(), "url": url},
                                   {"decision_number": "B-419046.2", "url": url},
                                   {"decision_number": number, "url": url.replace("%2C", ",")}], ["source"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["found", "not_in_table", "not_in_table", "not_in_table"]
    assert reply["keys"][1]["key"]["decision_number"] == "b-419046.2,b-419046.3"


def test_a_mixed_case_docket_id_is_matched_exactly_and_never_folded(tmp_path, monkeypatch):
    from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
    from spicy_regs.transforms.regulations_shape import shape_record

    raw = {**dict.fromkeys(name for name, _ in SOURCE_COLUMNS["dockets"]), "docket_id": "Treas-DO-2021-0008",
           "title": "A docket"}
    one_family(tmp_path, monkeypatch, {"dockets": [{**shape_record("dockets", raw), "input_metadata": {}}]})
    reply = call("dockets", [{"docket_id": "Treas-DO-2021-0008"}, {"docket_id": "TREAS-DO-2021-0008"},
                             {"docket_id": " Treas-DO-2021-0008"}], ["title"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["found", "not_in_table", "not_in_table"]
    assert reply["keys"][2]["key"] == {"docket_id": " Treas-DO-2021-0008"}


def test_a_numeric_looking_identifier_stays_text_and_a_number_given_for_it_is_refused(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"federal_register": [_register("2026-12696", html_url="https://fr.test/a"),
                                                            _register("12696", html_url="https://fr.test/b")]})
    reply = call("federal_register", [{"document_number": "2026-12696", "publication_date": "2026-06-25"},
                                      {"document_number": "12696", "publication_date": "2026-06-25"}], ["html_url"])
    assert [entry["fields"]["html_url"]["value"] for entry in reply["keys"]] == ["https://fr.test/a", "https://fr.test/b"]
    message = refused("federal_register", [{"document_number": 12696, "publication_date": "2026-06-25"}], ["html_url"])
    assert "gives document_number as int; it is text, so quote it" in message


def test_a_composite_key_takes_every_identity_field_with_its_integer_as_a_number_or_plain_digits(tmp_path, monkeypatch):
    def action(index: str) -> dict:
        return congress("bill_actions", bill_id="119-hr-1", action_index=index, source_system_code=f"code-{index}")

    one_family(tmp_path, monkeypatch, {"bill_actions": [action("3"), action("30")]})
    reply = call("bill_actions", [{"bill_id": "119-hr-1", "action_index": 3}, {"action_index": "30", "bill_id": "119-hr-1"},
                                  {"bill_id": "119-hr-1", "action_index": 3.0}, {"bill_id": "119-hr-1", "action_index": 4},
                                  {"bill_id": "119-HR-1", "action_index": 3}], ["source_system_code"])
    assert [entry["receipt"] for entry in reply["keys"]] == ["found", "found", "found", "not_in_table", "not_in_table"]
    assert [entry["fields"]["source_system_code"]["value"] for entry in reply["keys"][:3]] == ["code-3", "code-30", "code-3"]
    assert reply["keys"][1]["key"] == {"bill_id": "119-hr-1", "action_index": 30}
    assert reply["identity_fields"] == {"bill_id": "VARCHAR", "action_index": "BIGINT"}


# Which member a connection reads.

def test_the_member_read_is_the_table_family_member_in_each_kind_of_connection(tmp_path):
    work = tmp_path / "work"
    subject, receipts = written(work, "fcc_filings", [fcc("100")])
    index = index_of({"fcc-filings": published(tmp_path / "bucket", "fcc-filings", {"fcc_filings": subject},
                                               pq.read_table(receipts))})
    entry = index["families"]["fcc-filings"]
    path = f"{entry['prefix']}/etl_receipts.parquet"
    remote = lookup.selected_member(index, None, "https://data.example", "fcc_filings")
    assert remote == lookup.Member(f"https://data.example/{path}", entry["etlReceipts"]["sha256"], "fcc-filings",
                                   GENERATION, entry["artifactDigest"])
    assert lookup.selected_member(index, None, "https://data.example", "fcc_proceedings") is None
    # A family that publishes the table without receipts for it has none to read: not yet native, or a member
    # that lists other datasets only.
    legacy = {key: value for key, value in entry.items() if key != "etlReceipts"}
    assert lookup.selected_member(index_of({"fcc-filings": legacy}), None, "https://data.example", "fcc_filings") is None
    others = {**entry, "etlReceipts": {**entry["etlReceipts"], "datasets": ["fcc_proceedings"]}}
    assert lookup.selected_member(index_of({"fcc-filings": others}), None, "https://data.example", "fcc_filings") is None
    download = {"receipt_members": {path: "/local/receipts.parquet"}, "selected_tables": ["fcc_filings"], "native": {}}
    held = lookup.selected_member(index, download, "", "fcc_filings")
    assert held is not None and held.location == "/local/receipts.parquet"
    # A download that selected neither the table nor its family's member has nothing to read.
    assert lookup.selected_member(index, {**download, "selected_tables": []}, "", "fcc_filings") is None
    assert lookup.selected_member(index, {"receipt_members": {}, "selected_tables": ["fcc_filings"]}, "", "fcc_filings") is None
    native = {"native": {"fcc_filings": {"receipts": "/build/etl_receipts.parquet", "generation_id": "build-1"}}}
    assert lookup.selected_member(index_of({}), native, "", "fcc_filings") == lookup.Member(
        "/build/etl_receipts.parquet", None, None, "build-1", None)
    assert lookup.tables_with_receipts(index, None, "https://data.example") == ["fcc_filings"]


def test_the_tool_reads_through_a_built_connection_under_its_locked_file_access(tmp_path, monkeypatch):
    """A remote build with its real security settings: the footer and row-number reads are of an allowed member."""
    subject, receipts = written(tmp_path / "work", "fcc_filings", [fcc(str(n)) for n in range(100, 106)])
    root = tmp_path / "bucket"
    index = index_of({"fcc-filings": published(root, "fcc-filings", {"fcc_filings": subject},
                                               _descending(pq.read_table(receipts)), row_group=2)})
    monkeypatch.setattr(server, "R2_BASE_URL", str(root))
    monkeypatch.setattr(server, "TABLES", ("fcc_filings",))
    monkeypatch.setattr(server, "load_public_http", lambda con, retries: None)
    con = server._build_connection(server._Publication(index, None, None))
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    try:
        assert con.execute("SELECT current_setting('enable_external_access')").fetchone() == (False,)
        reply = call("fcc_filings", [{"id_submission": "103"}, {"id_submission": "nine"}], ["filing_url"])
        assert [entry["receipt"] for entry in reply["keys"]] == ["found", "not_in_table"]
        described = _tool_data(server.build_server(), "describe_table", {"table": "fcc_filings"})
        assert described["receipt_fields"]["available"] is True
    finally:
        con.close()


# describe_table: what to ask for, without provoking a refusal.

def test_describe_table_names_the_identity_and_receipt_fields_and_gives_meanings_on_detail(tmp_path, monkeypatch):
    one_family(tmp_path, monkeypatch, {"fcc_filings": [fcc("100")]})
    mcp = server.build_server()
    compact = _tool_data(mcp, "describe_table", {"table": "fcc_filings"})
    full = _tool_data(mcp, "describe_table", {"table": "fcc_filings", "detail": True})
    assert compact["receipt_fields"] == full["receipt_fields"]
    assert compact["receipt_fields"]["tool"] == "read_receipt_fields" and compact["receipt_fields"]["available"]
    assert compact["receipt_fields"]["identity_fields"] == {"id_submission": "VARCHAR"}
    # The fields the table lacks: not a column's own name, whose value is in the table and described there, and not
    # a list's former spelling where the table holds the list.
    held = {column["column_name"] for column in compact["columns"]}
    offered = compact["receipt_fields"]["fields"]
    renamed = lookup._declared()["fcc_filings"]["renamed"]
    assert renamed == {"authors_json": "authors", "bureaus_json": "bureaus", "documents_json": "documents",
                       "filers_json": "filers", "lawfirms_json": "lawfirms", "proceeding_names_json": "proceedings"}
    assert offered == [name for name in carried("fcc_filings") if name not in held and name not in renamed]
    assert {"filing_url", "native_fields_sha256", "native_fields_json", "raw_record"} <= set(offered)
    assert set(renamed.values()) <= held and not set(renamed) & set(offered)
    assert set(renamed) <= set(carried("fcc_filings"))  # still answered when asked for by name
    assert "id_submission" not in compact["receipt_fields"]["fields"] and "id_submission" in held
    assert "receipt_field_meanings" not in compact and "receipt_field_meanings" in compact["detail"]["omitted"]
    assert full["detail"]["omitted"] == []
    meanings = full["receipt_field_meanings"]
    assert meanings["filing_url"].startswith("Canonical fcc.gov URL for the filing")
    # Every meaning is of a named field; a field the dictionary gives no meaning has none stated.
    assert set(meanings) < set(compact["receipt_fields"]["fields"]) and "raw_record" not in meanings
    # A table whose family has published no receipts here still says what it would take, and that it is not here.
    unpublished = _tool_data(mcp, "describe_table", {"table": "congress_bills"})
    assert unpublished["receipt_fields"]["available"] is False
    assert unpublished["receipt_fields"]["identity_fields"] == {"bill_id": "VARCHAR"}
    assert "receipt_fields" not in _tool_data(mcp, "describe_table", {"table": "etl_receipts"})


def test_describing_receipt_fields_needs_no_pyarrow():
    program = '''
import importlib.abc, sys

class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyarrow', 'polars', 'spicy_docs'}:
            raise AssertionError(f'Optional source dependency imported: {fullname}')
        return None

sys.meta_path.insert(0, Block())
from spicy_regs import receipt_lookup
fields = receipt_lookup.carried_fields('fcc_filings')
assert fields['filing_url'] == 'raw_record' and fields['raw_record'] is None
compact, meanings = receipt_lookup.describe('fcc_filings', {'columns': [{'column_name': 'id_submission', 'column_type': 'VARCHAR'}]}, available=False)
assert compact['identity_fields'] == {'id_submission': 'VARCHAR'} and 'filing_url' in meanings
assert 'filing_url' in compact['fields'] and 'id_submission' not in compact['fields']
assert not any(name.startswith('spicy_regs.transforms') for name in sys.modules)
'''
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr


# The bundled declaration and the read markers it is built from.

def test_the_bundled_declaration_is_a_fresh_build_of_the_registries():
    assert declarations.record_errors() == [], "run 'uv run spicy-regs-dict generate'"


def test_every_installed_policy_family_has_a_field_registry_and_declared_mappings_are_receipt_fields():
    bundled = json.loads(declarations.RECORD.read_text(encoding="utf-8"))["tables"]
    keyed = {name for name, policy in POLICIES.items() if not policy.receipt_only and policy.identity_fields}
    assert set(bundled) == keyed
    for table, entry in bundled.items():
        policy = POLICIES[table]
        assert set(entry.get("containers", {})) <= set(policy.receipt_fields), table
        assert not set(entry.get("meanings", {})) & set(policy.subject_schema.names), table
        # A renamed field is one the receipts carry and the table does not, naming a column the table does hold.
        renamed = entry.get("renamed", {})
        assert set(renamed) <= set(carried(table)) - set(policy.subject_schema.names), table
        assert set(renamed.values()) <= set(policy.subject_schema.names), table
        for marker in entry.get("read_markers", ()):
            assert {*marker["columns"], *marker["fields"], *marker["where"]} <= set(carried(table)), table
    # The families the brief names each place a whole-row or conversion mapping; courts declare none.
    assert set(bundled["court_dockets"]) <= {"meanings", "renamed"}
    assert bundled["dockets"]["containers"].keys() == {"raw_conversion_inputs"}


def test_detail_read_governs_the_lists_the_views_and_the_index_builder_already_treat_as_detail_backed():
    from spicy_regs.relationship_views.congress import CONGRESS_RELATIONSHIPS
    from spicy_regs.relationship_views.regulatory import REGULATORY_RELATIONSHIPS
    from spicy_regs.transforms import build_congress_index as index

    markers = {marker.table: marker for marker in declarations.read_markers() if marker.columns == (index.DETAIL_READ,)}
    assert set(markers) == {table for table, spec in index.INDEX_SPECS.items() if spec.detail_route is not None}
    for view in (*CONGRESS_RELATIONSHIPS, *REGULATORY_RELATIONSHIPS):
        if view.detail_read_column:
            assert view.source_field in markers[view.source_table].fields
    for table, marker in markers.items():
        assert marker.read_value == "true" and marker.empty_unread
        # The builder's own statement of the lists a detail read that states none leaves NULL (_stale_sql).
        columns = set(index.INDEX_SPECS[table].shape({}, {})) - {index.DETAIL_READ}
        stale = index._stale_sql(columns, index.INDEX_SPECS[table])
        assert {column for column in columns if f"{column} = '[]'" in stale} <= set(marker.fields)


def test_the_federal_register_marker_names_the_nine_columns_its_dictionary_note_does():
    from spicy_regs.data_dictionary import load_descriptions
    from spicy_regs.schemas.regulations_subjects import RECEIPT_COLUMNS, SOURCE_COLUMNS

    governed = declarations.FEDERAL_REGISTER_READ
    assert len(governed) == 9 and set(governed) <= {name for name, _ in SOURCE_COLUMNS["federal_register"]}
    assert governed[0] in RECEIPT_COLUMNS["federal_register"]
    note = " ".join(load_descriptions()["federal_register"]["data_quality"].split())
    assert "is NULL in all nine of those columns, which means not read: test `regulations_dot_gov_info_json`" in note


def test_the_roll_call_markers_are_the_builder_s_read_columns_by_chamber():
    from spicy_regs.transforms.build_roll_call_votes import _READ_COLUMNS

    markers = {dict(marker.where)["chamber"]: marker for marker in declarations.read_markers()
               if marker.table == "roll_call_votes"}
    assert {chamber: marker.columns for chamber, marker in markers.items()} == dict(_READ_COLUMNS)
    assert all(marker.fields == marker.columns and marker.read_value is None for marker in markers.values())


def snapshot_serving(tmp_path, monkeypatch, *, edit=None):
    """Native subjects and internal receipts under a captured snapshot, with no family index entry."""
    from spicy_regs.sources import publication as pub

    rows = [{"proceeding_id": "proceeding_00003fe56396de15cc98430a", "method": "original method",
             "actor_id": None, "raw_conversion_inputs": {"literal": ["042", None, True]}},
            {"proceeding_id": "other", "method": "second"}]
    subject, receipts = written(tmp_path / "work", "proceedings", rows)
    root = tmp_path / "bucket"
    entry = published(root, "snapshot-fixture", {"proceedings": subject}, pq.read_table(receipts))
    snapshot_id = "snapshot_under_test"
    prefix = f"materialized/rulemaking/snapshots/{snapshot_id}"
    target = root / prefix
    target.parent.mkdir(parents=True)
    (root / entry["prefix"]).rename(target)
    artifacts = {key: {"remote_key": f"{prefix}/{key}", "sha256": value["sha256"].removeprefix("sha256:"),
                       "bytes": value["byteSize"], "rows": value["rows"], "visibility": "public"}
                 for key, value in entry["tables"].items()}
    receipt = entry["etlReceipts"]
    artifacts["etl_receipts.parquet"] = {
        "remote_key": f"{prefix}/etl_receipts.parquet", "sha256": receipt["sha256"].removeprefix("sha256:"),
        "bytes": receipt["byteSize"], "rows": receipt["rows"], "visibility": "internal"}
    manifest = {"dataset": "rulemaking", "format_version": 2, "snapshot_id": snapshot_id,
                "run_id": GENERATION, "asserted_at": "2026-10-05T00:00:00Z", "artifacts": artifacts,
                "etlReceipts": {"key": "etl_receipts.parquet", "generationId": GENERATION,
                                "policies": [POLICIES["proceedings"].descriptor()]}}
    if edit is not None:
        edit(manifest)
    manifest_key = f"{prefix}/manifest.json"
    pointer = {"dataset": "rulemaking", "format_version": 2, "snapshot_id": snapshot_id,
               "manifest_key": manifest_key}
    documents = {f"{root}/{pub.SNAPSHOT_POINTER}": pointer, f"{root}/{manifest_key}": manifest}
    snapshot = pub.load_rulemaking_snapshot(str(root), read=lambda url, **kwargs: json.dumps(documents[url]).encode())
    monkeypatch.setattr(server, "R2_BASE_URL", str(root))
    monkeypatch.setattr(server, "TABLES", ("proceedings",))
    monkeypatch.setattr(server, "load_public_http", lambda con, retries: None)
    con = server._build_connection(server._Publication(pub.empty_index(), snapshot, None))
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    return con, snapshot, target


def test_captured_snapshot_receipts_preserve_values_order_and_native_provenance(tmp_path, monkeypatch):
    from spicy_regs.sources import publication as pub

    con, snapshot, target = snapshot_serving(tmp_path, monkeypatch)
    assert con.execute("SELECT current_setting('enable_external_access')").fetchone() == (False,)
    allowed = con.execute("SELECT current_setting('allowed_paths')").fetchone()[0]
    assert str(target / "etl_receipts.parquet") in allowed
    monkeypatch.setattr(pub, "load_rulemaking_snapshot", lambda *a, **k: pytest.fail("lookup fetched a new snapshot"))
    key = {"proceeding_id": "proceeding_00003fe56396de15cc98430a"}
    reply = call("proceedings", [key, {"proceeding_id": "other"}, key, {"proceeding_id": "absent"}],
                 ["method", "actor_id", "raw_conversion_inputs"])
    assert [item["key"] for item in reply["keys"]] == [key, {"proceeding_id": "other"}, key, {"proceeding_id": "absent"}]
    assert reply["keys"][0] == reply["keys"][2]
    assert reply["keys"][0]["fields"] == {
        "method": {"state": "stated", "value": "original method"}, "actor_id": {"state": "null_unmarked"},
        "raw_conversion_inputs": {"state": "stated", "value": {"literal": ["042", None, True]}}}
    assert reply["keys"][3]["receipt"] == "not_in_table"
    assert reply["receipts"] == {
        "family": None, "generation_id": GENERATION, "artifact_digest": None,
        "snapshot_id": snapshot["snapshot_id"], "manifest_key": snapshot["manifest_key"],
        "member_sha256": "sha256:" + snapshot["manifest"]["artifacts"]["etl_receipts.parquet"]["sha256"],
        "policy_version": POLICIES["proceedings"].policy_version}
    assert _tool_data(server.build_server(), "describe_table", {"table": "proceedings"})["receipt_fields"]["available"]
    assert "proceedings" in lookup.tables_with_receipts(pub.empty_index(), None, str(target), snapshot)
    assert "gives no proceeding_id" in refused("proceedings", [{}], ["method"])
    assert "do not carry 'not_a_field'" in refused("proceedings", [key], ["not_a_field"])
    monkeypatch.setattr(lookup, "SCAN_ROW_BOUND", 1)
    assert "over the 1-receipt bound" in refused("proceedings", [key], ["method"])
    con.close()


@pytest.mark.parametrize("change", [
    lambda m: m["etlReceipts"].__setitem__("generationId", "another-run"),
    lambda m: m["etlReceipts"].__setitem__("key", "../other.parquet"),
    lambda m: m["artifacts"]["etl_receipts.parquet"].__setitem__("remote_key", "materialized/other/etl_receipts.parquet"),
    lambda m: m["artifacts"]["etl_receipts.parquet"].__setitem__("sha256", "missing"),
    lambda m: m["artifacts"]["etl_receipts.parquet"].__setitem__("visibility", "public"),
    lambda m: m["artifacts"].pop("etl_receipts.parquet"),
    lambda m: m["etlReceipts"].__setitem__("policies", [{"dataset": "proceedings"}, {"dataset": "proceedings"}]),
])
def test_a_misbound_snapshot_receipt_refuses_at_snapshot_admission(tmp_path, monkeypatch, change):
    from spicy_regs.sources.publication import PublicationError

    with pytest.raises(PublicationError):
        snapshot_serving(tmp_path, monkeypatch, edit=change)


def test_snapshot_selection_follows_subject_precedence_and_never_supplies_local_receipts(tmp_path, monkeypatch):
    from spicy_regs.sources import publication as pub

    con, snapshot, _ = snapshot_serving(tmp_path, monkeypatch)
    index = pub.empty_index()
    assert lookup.selected_member(index, {}, "unused", "proceedings", snapshot) is None
    assert lookup.selected_member(index, None, "unused", "rule_targets", snapshot) is None
    # A legacy snapshot with no declared receipt remains honestly unavailable.
    legacy = json.loads(json.dumps(snapshot))
    legacy["manifest"].pop("etlReceipts")
    assert lookup.selected_member(index, None, "unused", "proceedings", legacy) is None
    # A family entry owns the subject even if it has no receipts; a snapshot must not fill that gap.
    index["families"]["new-owner"] = {"tables": {"proceedings.parquet": {}}, "prefix": "new-owner"}
    assert lookup.selected_member(index, None, "unused", "proceedings", snapshot) is None
    con.close()


@pytest.mark.parametrize('selection', ['download', 'native'])
def test_indexed_local_selection_full_reply_parity(tmp_path, monkeypatch, selection):
    con = one_family(tmp_path, monkeypatch, {'fcc_filings': [
        fcc('100', text_data='', native_fields_json=None), fcc('200', text_data='held')]})
    keys = [{'id_submission': '200'}, {'id_submission': '100'}, {'id_submission': '200'}]
    fields = ['filing_url', 'native_fields_json', 'text_data']
    expected = call('fcc_filings', keys, fields)
    _, entry = _add_selected_index(con)
    receipt_path = entry['prefix'] + '/' + entry['etlReceipts']['key']
    index_path = entry['prefix'] + '/' + entry['etlReceipts']['keyIndex']['key']
    receipt_location = str(tmp_path / receipt_path)
    index_location = str(tmp_path / index_path)
    local: dict[str, Any]
    if selection == 'download':
        local = {'selected_tables': ['fcc_filings'], 'receipt_members': {receipt_path: receipt_location},
                 'receipt_indexes': {index_path: index_location}}
    else:
        local = {'native': {'fcc_filings': {'receipts': receipt_location,
                  'generation_id': entry['etlReceipts']['generationId'], 'key_index': index_location,
                  'key_index_descriptor': entry['etlReceipts']['keyIndex']}}}
    # Fixture remote-style files are rooted at the actual server URL, not necessarily tmp_path.
    from pathlib import Path
    local_receipt = str(Path(server.R2_BASE_URL) / receipt_path)
    local_index = str(Path(server.R2_BASE_URL) / index_path)
    if selection == 'download':
        local['receipt_members'][receipt_path] = local_receipt
        local['receipt_indexes'][index_path] = local_index
    else:
        local['native']['fcc_filings'].update(receipts=local_receipt, key_index=local_index)
    from spicy_regs.local_data import file_signature
    local['signatures'] = {str(path): file_signature(path) for path in
                           (Path(server.R2_BASE_URL) / entry['prefix']).glob('*.parquet')}
    local.setdefault('selected_tables', ['fcc_filings'])
    local['directory'] = str(tmp_path)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, '_connection_local_selection', lambda cursor: local)
    monkeypatch.setattr(lookup, '_accepted', lambda *a: pytest.fail('Indexed local lookup scanned receipts'))
    monkeypatch.setattr(lookup, '_stated_identities', lambda *a: pytest.fail('Indexed local lookup scanned policy payloads'))
    actual = call('fcc_filings', keys, fields)
    expected.pop('base_url', None)
    expected.update(source='local', base_path=str(tmp_path), selected_directory=str(tmp_path))
    # Native selections identify their own publisher, without a remote family artifact.
    if selection == 'native':
        expected['receipts'] = {**expected['receipts'], 'family': None, 'artifact_digest': None}
    assert actual == expected


@pytest.mark.parametrize('selection', ['download', 'native'])
def test_local_declared_index_missing_refuses_without_scan(tmp_path, monkeypatch, selection):
    from pathlib import Path
    con = one_family(tmp_path, monkeypatch, {'fcc_filings': [fcc('100')]})
    _, entry = _add_selected_index(con)
    path = entry['prefix'] + '/' + entry['etlReceipts']['key']
    local: dict[str, Any]
    if selection == 'download':
        local = {'selected_tables': ['fcc_filings'],
                 'receipt_members': {path: str(Path(server.R2_BASE_URL) / path)}, 'receipt_indexes': {}}
        message = 'Declared receipt key index is absent'
    else:
        local = {'native': {'fcc_filings': {'receipts': str(Path(server.R2_BASE_URL) / path),
                 'generation_id': entry['etlReceipts']['generationId'], 'key_index': None,
                 'key_index_descriptor': entry['etlReceipts']['keyIndex']}}}
        message = 'Selected native receipt index declaration is incomplete'
    from spicy_regs.local_data import file_signature
    local['signatures'] = {str(path): file_signature(path) for path in
                           (Path(server.R2_BASE_URL) / entry['prefix']).glob('*.parquet')}
    local.setdefault('selected_tables', ['fcc_filings'])
    local['directory'] = str(tmp_path)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, '_connection_local_selection', lambda cursor: local)
    monkeypatch.setattr(lookup, '_accepted', lambda *a: pytest.fail('Declared index fell back to scan'))
    assert message in refused('fcc_filings', [{'id_submission': '100'}], ['filing_url'])


def test_indexed_mixed_versions_use_only_representative_receipt_positions(tmp_path, monkeypatch):
    installed = POLICIES['fcc_filings']
    _under(tmp_path, monkeypatch, [(installed, [fcc('100')]),
           (replace(installed, policy_version='government-sources/0'), [fcc('200')])])
    con = server._get_connection()
    _add_selected_index(con)
    monkeypatch.setattr(lookup, '_stated_identities', lambda *a: pytest.fail('Indexed mixed-policy payload scan'))
    monkeypatch.setattr(lookup, 'SCAN_ROW_BOUND', 0)
    result = call('fcc_filings', [{'id_submission': '100'}], ['filing_url'])
    assert result['keys'][0]['receipt'] == 'found'
    assert result['receipts']['policy_version'] == ['government-sources/0', installed.policy_version]


def test_indexed_unrequested_incompatible_policy_version_refuses(tmp_path, monkeypatch):
    installed = POLICIES['fcc_filings']
    other = replace(installed, identity_fields=('id_submission', 'total_page_count'),
                    policy_version='government-sources/8')
    _under(tmp_path, monkeypatch, [(installed, [fcc('100')]), (other, [fcc('200', total_page_count='4')])])
    _add_selected_index(server._get_connection())
    monkeypatch.setattr(lookup, '_stated_identities', lambda *a: pytest.fail('Indexed identity payload scan'))
    message = refused('fcc_filings', [{'id_submission': '100'}], ['filing_url'])
    assert "'government-sources/8'" in message and 'total_page_count (integer)' in message


def test_indexed_receipt_reply_retains_all_detail_field_states(tmp_path, monkeypatch):
    rows = []
    for number, (read, values) in enumerate([
        ('false', dict.fromkeys(MEETING_LISTS)), ('true', dict.fromkeys(MEETING_LISTS)),
        (None, dict.fromkeys(MEETING_LISTS)), ('true', dict.fromkeys(MEETING_LISTS, '[]')),
        ('true', MEETING_LISTS)], 1):
        rows.append(congress('committee_meetings', congress='119', chamber='house', event_id=str(number),
                             detail_read=read, **values))
    con = one_family(tmp_path, monkeypatch, {'committee_meetings': rows})
    keys = [{'congress': '119', 'chamber': 'house', 'event_id': str(n)} for n in range(1, 6)]
    fields = list(MEETING_LISTS)
    expected = call('committee_meetings', keys, fields)
    _add_selected_index(con)
    monkeypatch.setattr(lookup, '_accepted', lambda *a: pytest.fail('Indexed field-state lookup scanned receipts'))
    assert call('committee_meetings', keys, fields) == expected
    assert set(expected['state_meaning']) >= {'unread', 'not_stated', 'stated_empty', 'stated'}
