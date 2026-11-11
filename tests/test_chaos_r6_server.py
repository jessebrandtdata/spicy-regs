"""Round-6 chaos repairs at the server boundary: strict arguments, read status, lineage, reply size, agency text.

Evidence: corpora/mcp-chaos-2026-10-02/round6/phase2-server.md (H1, H2, H4, M8, L7, L9, L10) and phase3-review.md
(the review's changes and the owner's decisions: refuse an oversized reply with its remedy, and shrink replies).
"""

from __future__ import annotations

import asyncio
import json
import re

import duckdb
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import acquisition_queue, mcp_server as server
from spicy_regs.citation_resolution import _occurrence_key
from tests.test_chaos_r5_server import (DOCS_LIVE, HELD, _cite, _documents_parent, _held_field_reads, _index,
                                        _lineage, _resolve, _resolve_held)
from tests.test_mcp_relationships import citation_connection, merged_occurrences
from tests.test_mcp_server import _tool_data

#: One valid call per tool, so a test can add a single bad argument to it.
VALID = {
    "connection_status": {},
    "list_sources": {},
    "describe_table": {"table": "laws"},
    "query_sql": {"sql": "SELECT 1"},
    "lookup_agency": {"namespace": "regulations.gov:agency", "identifier": "OPM"},
    "resolve_document_citations": {"document_kind": "govinfo_package", "document_key": "CRPT-example"},
}


def _call(name, arguments):
    return asyncio.run(server.build_server().call_tool(name, arguments))


def _refusal(monkeypatch, name, arguments) -> str:
    monkeypatch.setattr(server, "_get_connection", lambda: pytest.fail(f"{name} ran with a bad argument"))
    with pytest.raises(ToolError) as refused:
        _call(name, arguments)
    return str(refused.value)


# H4, L9, L7: an argument a tool does not declare, or a value outside its bound, is refused in written words.

def test_every_tool_advertises_that_it_takes_no_other_argument():
    tools = asyncio.run(server.build_server().list_tools())
    assert {tool.name for tool in tools} == set(VALID)
    for tool in tools:
        assert tool.input_schema.get("additionalProperties") is False, tool.name
        assert "title" not in tool.input_schema and not any("title" in field for field in
                                                            tool.input_schema["properties"].values()), tool.name


@pytest.mark.parametrize("name", sorted(VALID))
def test_every_tool_refuses_an_argument_it_does_not_declare(monkeypatch, name):
    message = _refusal(monkeypatch, name, {**VALID[name], "offest": 50})
    assert "offest is not an argument." in message and f"{name} takes" in message


def test_a_misspelled_argument_is_refused_naming_the_arguments(monkeypatch):
    """zubair's offest ran page 0 with nothing said (round 6)."""
    message = _refusal(monkeypatch, "query_sql", {"sql": "SELECT 1", "offest": 50})
    assert message == ("Error executing tool query_sql: offest is not an argument. "
                       "query_sql takes max_cell_chars, max_rows, sql.")


def test_a_tool_with_no_arguments_says_it_takes_none(monkeypatch):
    message = _refusal(monkeypatch, "connection_status", {"detail": True})
    assert message == ("Error executing tool connection_status: detail is not an argument. "
                       "connection_status takes no arguments.")


def test_list_sources_names_its_one_argument(monkeypatch):
    message = _refusal(monkeypatch, "list_sources", {"detail": True})
    assert message == "Error executing tool list_sources: detail is not an argument. list_sources takes subject."


def test_a_bound_is_stated_in_plain_words(monkeypatch):
    message = _refusal(monkeypatch, "query_sql", {"sql": "SELECT 1", "max_rows": 501})
    assert message == ("Error executing tool query_sql: max_rows: Input should be less than or equal to 500. "
                       "query_sql takes max_cell_chars, max_rows, sql.")


def test_an_unknown_argument_a_bad_value_and_a_missing_one_are_said_together(monkeypatch):
    message = _refusal(monkeypatch, "query_sql", {"max_rows": 0, "limit": 5})
    assert message == ("Error executing tool query_sql: limit is not an argument; sql is required; max_rows: Input "
                       "should be greater than or equal to 1. query_sql takes max_cell_chars, max_rows, sql.")


def test_an_unsupported_namespace_is_refused_naming_the_supported_ones(monkeypatch):
    """Reverses rounds 4-5: an unsupported namespace answered success with status unsupported_namespace."""
    message = _refusal(monkeypatch, "lookup_agency", {"namespace": "sec.gov:cik", "identifier": "OPM"})
    assert "namespace: Input should be 'regulations.gov:agency' or 'federal_register_agency'" in message
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "lookup_agency"]
    assert tool.input_schema["properties"]["namespace"]["enum"] == ["regulations.gov:agency", "federal_register_agency"]


# M8: a held field's read recorded before rule sets were kept is a read, and status words are defined where used.

def test_a_zero_row_read_recorded_without_a_rule_set_or_time_is_read_none_found(monkeypatch):
    """Every published document_citation_reads row states no rule set and no read time (round 6: 13 of 13, the
    dictionary calls that a read recorded before the table existed); court opinion 11264529 read 0 rows."""
    with citation_connection() as con:
        _held_field_reads(con, (HELD, None, None, 0))
        result = _resolve_held(con, monkeypatch)
    assert result["source_read"] == {"table": "comments", "status": "read_none_found"}
    assert result["coverage"]["partial"] is False


def test_an_undated_read_stating_rows_that_are_not_held_still_refuses(monkeypatch):
    """Reads of one text that state no time are ordered by their rows, so one stating rows is never hidden by a 0."""
    with citation_connection() as con, pytest.raises(ToolError, match="states 4 citation rows"):
        _held_field_reads(con, (HELD, None, None, 0), (HELD, None, None, 4))
        _resolve_held(con, monkeypatch)


def test_the_reply_defines_exactly_the_target_status_words_it_uses(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "119-public-999", "30"), ("crs_report_id", "R12345", "40"))
        result = _resolve(con, monkeypatch)
        found = _resolve(con, monkeypatch, cite_kind="public_law", max_occurrences=2)
    used = {row["target_status"] for row in merged_occurrences(result)}
    assert used == {"found", "missing", "not_checked"} == set(result["target_status_meaning"])
    assert "may not cover" in result["target_status_meaning"]["missing"]
    assert "reason" in result["target_status_meaning"]["not_checked"]
    assert set(found["target_status_meaning"]) == {"found"}
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "resolve_document_citations"]
    assert "target_status_meaning" in (tool.description or "")


# H2: inputs_current is three-valued, a family's own earlier output is its prior generation, an export parent is
# compared with the receipt the connection pinned, and a snapshot table is never stale for a source it may not read.

def _described(name="discovery_signals"):
    return _tool_data(server.build_server(), "describe_table", {"table": name})["publication"]


def test_a_current_input_and_an_unknown_one_are_unknown_not_current(monkeypatch):
    parents = {**_documents_parent(DOCS_LIVE, "sha256:" + "d" * 64), "comments.parquet": {"etag": '"e"', "byteSize": 9}}
    con, _ = _lineage(monkeypatch, parents)
    with con:
        described = _described()
    assert [item["input_table_current"] for item in described["inputs"]] == [None, True]
    assert described["inputs_current"] is None


def test_a_lagging_input_is_stale_whatever_else_is_unknown(monkeypatch):
    con, _ = _lineage(monkeypatch, {**_documents_parent(), "comments.parquet": {"etag": '"e"', "byteSize": 9}})
    with con:
        assert _described()["inputs_current"] is False


def test_the_familys_own_earlier_output_is_its_prior_generation_never_a_stale_input(monkeypatch):
    """gao-reports records its own previous generation as a parent of both its tables (round 6: read as a lag)."""
    prior = "sha256:" + "9" * 64
    own = {f"{table}.parquet": {"family": "discovery-signals", "artifactDigest": prior, "sha256": "sha256:" + "8" * 64,
                                "byteSize": 1} for table in ("discovery_signals", "signal_history")}
    con, _ = _lineage(monkeypatch, own)
    with con:
        described = _described()
    assert described["prior_generation"] == prior
    assert "inputs" not in described and "inputs_current" not in described
    con, _ = _lineage(monkeypatch, {**own, **_documents_parent()})
    with con:
        described = _described()
    assert described["prior_generation"] == prior and [item["table"] for item in described["inputs"]] == ["documents"]
    assert described["inputs_current"] is False


def _exports(matches=True, **receipt):
    return {"comments_index": {
        "rows": 3 if matches else None,
        "rows_basis": "comments_export_receipt" if matches else "export_receipt_does_not_match_object",
        "export_receipt": {"receipt_sha256": "sha256:" + "1" * 64, "sha256": "sha256:" + "7" * 64, "etag": '"x"',
                           "bytes": 9, "catalog_snapshot_id": "5", **receipt}}}


@pytest.mark.parametrize(("parent", "exports", "live", "current"), [
    ({"sha256": "sha256:" + "7" * 64}, _exports(), "sha256:" + "7" * 64, True),
    ({"sha256": "sha256:" + "6" * 64}, _exports(), "sha256:" + "7" * 64, False),
    ({"etag": '"x"'}, _exports(), '"x"', True),
    ({"etag": '"x"'}, _exports(matches=False), None, None),  # the file moved since its receipt: nothing to compare
    ({"sha256": "sha256:" + "7" * 64}, {}, None, None),  # no receipt pinned
])
def test_an_export_parent_is_compared_with_the_receipts_pin_of_the_same_kind(monkeypatch, parent, exports, live,
                                                                              current):
    con, _ = _lineage(monkeypatch, {"comments_index.parquet": {**parent, "byteSize": 9}})
    monkeypatch.setattr(server, "_export_rows", lambda cursor: exports)
    with con:
        described = _described()
    assert described["inputs"] == [{"table": "comments_index", "family": None, "built_from": next(iter(parent.values())),
                                    "live": live, "input_table_current": current}]
    assert described["inputs_current"] is current


def _snapshot(con, sources, previous="snapshot_" + "5" * 32):
    """Pin a rulemaking snapshot holding comment_periods, whose manifest records ``sources``."""
    manifest = {"snapshot_id": "snapshot_" + "6" * 32, "run_id": "r", "asserted_at": "2026-10-03T16:06:23Z",
                "inputs": {"previous_snapshot_id": previous, "sources": sources},
                "stages": [{"name": "comment-periods", "depends_on": ["proceedings"],
                            "outputs": ["comment_periods.parquet"]}]}
    pinned = {"snapshot_id": manifest["snapshot_id"], "manifest": manifest,
              "tables": {"comment_periods.parquet": {"sha256": "4" * 64, "rows": 2}}}
    con.execute("CREATE TABLE _spicy_rulemaking (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_rulemaking VALUES (?)", [json.dumps(pinned)])
    con.execute("CREATE TABLE comment_periods (id VARCHAR)")


def test_a_snapshot_table_is_never_stale_for_a_source_it_may_not_have_read(monkeypatch):
    """The manifest records sources per snapshot and stages without theirs, so which sources a table read is unknown:
    a lagging source leaves every table of the snapshot unknown, never false (phase3-review item 3)."""
    con, _ = _lineage(monkeypatch, {})
    with con:
        _snapshot(con, {"documents.parquet": {"sha256": "f" * 64, "bytes": 1},
                        "unified_agenda.parquet": {"sha256": "a" * 64, "bytes": 1}})
        described = _described("comment_periods")
    assert described["prior_generation"] == "snapshot_" + "5" * 32
    assert described["snapshot_inputs"] == [
        {"table": "documents", "built_from": "sha256:" + "f" * 64, "live": "sha256:" + "d" * 64,
         "input_table_current": False},
        {"table": "unified_agenda", "built_from": "sha256:" + "a" * 64, "live": None, "input_table_current": None}]
    assert "inputs" not in described and described["inputs_current"] is None


def test_a_snapshot_whose_sources_are_all_live_is_current(monkeypatch):
    con, _ = _lineage(monkeypatch, {})
    with con:
        _snapshot(con, {"documents.parquet": {"sha256": "d" * 64, "bytes": 1}})
        described = _described("comment_periods")
    assert described["snapshot_inputs"] == [{"table": "documents", "built_from": "sha256:" + "d" * 64,
                                             "live": "sha256:" + "d" * 64, "input_table_current": True}]
    assert described["inputs_current"] is True


@pytest.mark.parametrize(("instant", "basis"), [
    ("2026-10-03T16:13:56Z", "last_object_write"),  # written by the 2026-10-03 backfill
    ("2026-10-01T03:12:26Z", "last_object_write"),
    ("2026-10-03T16:13:56.500000Z", "pointer_move"),  # compared as instants, not as text
    ("2026-10-03T16:41:21Z", "pointer_move"),
    (None, None),
])
def test_published_at_names_what_was_observed(monkeypatch, instant, basis):
    with citation_connection() as con:
        family = {"publishedAt": instant} if instant else {}
        con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(_index(**family))])
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        described = _described("laws")
    assert (described["published_at"], described["published_at_basis"]) == (instant, basis)


@pytest.mark.parametrize("name", ["describe_table", "query_sql"])
def test_the_texts_say_inputs_are_what_the_producer_recorded(name):
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == name]
    text = " ".join((tool.description or "").split())  # a client reads the wrapped lines as one paragraph
    assert "No recorded parents does not mean no inputs" in text
    assert "reads bypassing the download helper are not recorded" in text


# H1 (owner decision 2026-10-03, "refuse with the remedy" and "shrink replies"): rows are arrays under one column
# list; a reply past the budget is refused with how many rows fit and how to ask again; nothing partial for size.

def _query(monkeypatch, sql, **arguments):
    con = duckdb.connect()
    con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps({"families": {}})])
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    return _tool_data(server.build_server(), "query_sql", {"sql": sql, **arguments})


def _size(reply) -> int:
    return len(json.dumps(reply, separators=(",", ":"), ensure_ascii=False))


def test_rows_are_arrays_in_the_order_columns_names_once(monkeypatch):
    reply = _query(monkeypatch, "SELECT i, 'x' || i AS label FROM range(3) t(i) ORDER BY i")
    assert reply["columns"] == ["i", "label"]
    assert reply["rows"] == [[0, "x0"], [1, "x1"], [2, "x2"]]


PADDED = "SELECT i, repeat('x', 100) AS pad FROM range(50) t(i) ORDER BY i"


def test_a_query_reply_past_the_budget_is_refused_with_the_rows_that_fit(monkeypatch):
    monkeypatch.setattr(server, "REPLY_CHARS", 2_000)
    with pytest.raises(ToolError) as refused:
        _query(monkeypatch, PADDED, max_rows=50)
    message = str(refused.value)
    found = re.search(r"The first (\d+) of its 50 rows fit", message)
    assert found, message
    fit = int(found[1])
    assert 0 < fit < 50 and "over the 2,000-character reply limit; nothing is returned" in message
    assert f"ORDER BY a unique key and LIMIT {fit}, then LIMIT {fit} OFFSET {fit}" in message
    assert "max_cell_chars" in message and "pad holds" in message
    # The remedy works: those rows, asked for again with the clause the refusal names, fit.
    page = _query(monkeypatch, PADDED.replace("ORDER BY i", f"ORDER BY i LIMIT {fit} OFFSET {fit}"), max_rows=50)
    assert page["row_count_shown"] == fit and page["rows"][0][0] == fit and _size(page) <= 2_000


def test_a_query_whose_first_row_is_past_the_budget_names_the_widest_column(monkeypatch):
    monkeypatch.setattr(server, "REPLY_CHARS", 2_000)
    with pytest.raises(ToolError, match=r"Not even the first row fits: set max_cell_chars to cut long cells \(big "
                                        r"holds 100% of the rows' characters\)"):
        _query(monkeypatch, "SELECT 1 AS id, repeat('x', 5000) AS big")


def test_a_reply_within_the_budget_is_whole_and_the_max_rows_cut_is_unchanged(monkeypatch):
    monkeypatch.setattr(server, "REPLY_CHARS", 2_000)
    reply = _query(monkeypatch, "SELECT i FROM range(30) t(i) ORDER BY i", max_rows=10)
    assert reply["rows"] == [[i] for i in range(10)] and reply["truncated"] is True and _size(reply) <= 2_000


def test_the_budget_is_set_by_the_environment_and_must_be_a_positive_count(monkeypatch):
    assert server.REPLY_CHARS == 40_000
    monkeypatch.setenv("SPICY_REGS_REPLY_CHARS", "25000")
    assert server._resolve_reply_chars() == 25_000
    monkeypatch.setenv("SPICY_REGS_REPLY_CHARS", "lots")
    with pytest.raises(RuntimeError, match="SPICY_REGS_REPLY_CHARS"):
        server._resolve_reply_chars()


def _many_citations(con, count=40):
    _cite(con, *[("public_law", f"93-public-{n}", str(100 + n)) for n in range(count)])


def test_a_citation_page_past_the_budget_is_refused_with_the_occurrences_that_fit(monkeypatch):
    monkeypatch.setattr(server, "REPLY_CHARS", 6_000)
    with citation_connection() as con:
        _many_citations(con)
        with pytest.raises(ToolError) as refused:
            _resolve(con, monkeypatch, max_occurrences=42)
        message = str(refused.value)
        found = re.search(r"The first (\d+) of its 42 occurrences fit", message)
        assert found, message
        fit = int(found[1])
        assert 0 < fit < 42 and f"max_occurrences={fit} and offset=0, then offset={fit}" in message
        page = _resolve(con, monkeypatch, max_occurrences=fit, offset=fit)
    assert len(page["occurrences"]) == fit and _size(page) <= 6_000


def test_the_queue_names_each_requesting_occurrence_by_its_span_without_losing_any(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "93-public-344", "40"), ("public_law", "93-public-344", "50"),
              ("public_law", "94-public-1", "60"))
        index = _index()
        index["families"]["print-citations"] = {**index["families"]["laws"], "tables": {
            "house_activity_reports.parquet": index["families"]["laws"]["tables"]["laws.parquet"]}}
        con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(index)])
        captured, build = {}, acquisition_queue.build_missing_target_queue

        def keep(*args, **kwargs):  # the whole queue the reply is projected from
            captured["queue"] = build(*args, **kwargs)
            return captured["queue"]

        monkeypatch.setattr(acquisition_queue, "build_missing_target_queue", keep)
        result = _resolve(con, monkeypatch)
    queue, full = result["acquisition_queue"], captured["queue"]
    shared = queue["shared_fields"]
    occurrences = merged_occurrences(result)
    rebuilt = []
    for item in queue["items"]:
        merged = {**shared["item"], **shared["by_target_kind"][item["target_kind"]], **item}
        route = merged["provider_route"]
        merged["provider_route"] = {**route, "native_identifier": route.get("native_identifier",
                                                                            merged["normalized_key"])}
        merged["requesting_occurrences"] = [
            {"occurrence_key": _occurrence_key(row), **{field: row.get(field) for field in REQUEST_FIELDS},
             "input_snapshot": shared["input_snapshot"]}
            for span in merged.pop("requesting_spans") for row in occurrences
            if (row["cite_kind"], row["target_key"], row["span_start"]) == (item["target_kind"],
                                                                             item["normalized_key"], span)]
        rebuilt.append(merged)
    assert rebuilt == full["items"] and [item["requesting_spans"] for item in queue["items"]] == [["40", "50"], ["60"]]
    assert "resolution_coverage" not in queue["coverage"] and full["coverage"]["resolution_coverage"] == result["coverage"]


REQUEST_FIELDS = ("document_kind", "document_key", "matched_text", "text_sha256", "span_start", "span_end",
                  "resolution_rule", "source_status")


def test_an_occurrence_drops_its_key_digest_and_a_rule_named_for_its_kind_and_merges_back():
    row = {"document_kind": "govinfo_package", "document_key": "D", "text_sha256": "t", "cite_kind": "rin",
           "target_key": "0648-AC64", "span_start": "4", "target_kind": "rin", "normalized_key": "0648-AC64",
           "target_rule": "rin", "rule_version": "002", "target_status": "found", "reason": None}
    rows = [{**row, "occurrence_key": _occurrence_key(row)},
            {**row, "span_start": "9", "target_rule": "rin_fallback", "occurrence_key": _occurrence_key({**row,
                                                                                                    "span_start": "9"})}]
    compact, fields = server._compact_occurrences(rows)
    assert all("occurrence_key" not in item for item in compact)
    assert "target_rule" not in compact[0] and compact[1]["target_rule"] == "rin_fallback"
    assert fields["by_cite_kind"]["rin"]["rule_version"] == "002"
    merged = merged_occurrences({"occurrences": compact, "occurrence_fields": fields})
    assert [{**item, "occurrence_key": _occurrence_key(item)} for item in merged] == rows


def test_a_file_without_a_same_as_field_is_not_given_one():
    """A legacy document_citations file has no target_rule; merging back must not invent it from cite_kind."""
    compact, fields = server._compact_occurrences([{"cite_kind": "rin", "target_key": "k", "span_start": "1"}])
    assert "target_rule" not in fields["same_as"]


#: The default list_sources reply's bound: table names by subject, never table entries (owner decision 2026-10-31,
#: ss-zt4p, raised from ss-ehe5's 2,000 for counts only).
DEFAULT_REPLY_CHARS = 10_000
#: What a live connection adds to a description that the dictionary-typed fixture lacks: pins with inputs (a
#: snapshot table's are about 1,600 characters, round 6) and release summaries. Measured 2026-10-03 on the live
#: bucket: list_sources 34,243 characters, the largest default description congress_bills' 27,748 (27,476 here).
LIVE_ROOM = 5_000


def test_the_catalog_replies_stay_under_the_budget_so_none_is_ever_refused_or_cut(monkeypatch):
    """list_sources and describe_table are not row-shaped: nothing refuses or cuts them, so their size is held here."""
    from tests.test_chaos_r3_server import _typed_tables

    record, _ = server._ledger()
    monkeypatch.setattr(server, "R2_BASE_URL", record["destination"])  # so descriptions carry the ledger's audits
    with duckdb.connect() as con:
        metadata = server._table_metadata()
        _typed_tables(con, [table for table in server.TABLES if table in metadata])
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        listed = _tool_data(mcp, "list_sources", {})
        # Discovery contains row counts and a compact release identity, not describe_table's parent inventories.
        # Paged by subject (owner decision 2026-10-31, ss-ehe5): the whole catalog had reached 39,135 characters, its
        # reserve cut from 2,000 to 1,000 to 500; each page now keeps 1,000, and descriptions retain their 5,000.
        # A signed 64-bit row count uses 19 characters versus NULL's 4, for every table at once. The two fixed-URL
        # exports also state their count basis. Both release statuses are included at their maximum count width.
        # Cold discovery can defer every qualified view: names remain complete on the views page while the common
        # admission reason is stated once per group, and the default reply states its count.
        widest, qualified = (1 << 63) - 1, [spec.view.name for spec in server.FEC_QUALIFIED_VIEWS]
        statuses = {"compatible": len(qualified), "disabled": len(qualified)}
        reason = "Release pins match; exact receipt row and output schema admission remains pending."
        listed["fec_release"]["status_counts"] = statuses
        listed["deferred_views"] = [{"count": len(qualified), "query_admission": "pending_receipt_rows", "reason": reason}]
        # The fixture loads every declared table; live, ten were unloaded (2026-10-31). Widen to the longest names.
        assert listed["unavailable_tables"] == [] and listed["unavailable_table_count"] == 0
        listed["unavailable_tables"] = sorted(server.TABLES, key=len, reverse=True)[:server.UNAVAILABLE_TABLES_LISTED]
        listed["unavailable_table_count"] = len(server.TABLES)
        listed["unavailable_views"] = 1 << 20
        # The fixture binds no archive; the default carries its status and view count (the archive test bounds the page).
        listed["archive"] = {"status": "unavailable", "views": 1 << 20}
        pages, listed["pages"] = listed["pages"], ["views", "archive"]
        assert _size(listed) <= DEFAULT_REPLY_CHARS
        assert pages == ["views"]
        for page in [*(group["subject"] for group in listed["subjects"]), *pages]:
            reply = _tool_data(mcp, "list_sources", {"subject": page})
            for group in reply.get("subjects", []):
                for table in group["tables"]:
                    table["rows"] = widest
                    if table["table"] in {"comments", "comments_index"}:
                        table["rows_basis"] = "export_receipt_does_not_match_object"
            if page == "views":
                from spicy_regs.fec_release import runtime_consumer

                reply["fec_release"].update(receipt_sha256="sha256:" + "a" * 64, status_counts=statuses,
                                            consumer=runtime_consumer("sha256:" + "b" * 64))
                reply["deferred_views"].append({"views": qualified, "query_admission": "pending_receipt_rows",
                                                "reason": reason})
                reply["unavailable_tables"] = list(server.TABLES)
            assert _size(reply) <= server.REPLY_CHARS - 1_000, page
        sizes = {}
        for name in [*server.TABLES, *server._connection_relationships(con.cursor())]:
            for detail in (False, True):
                sizes[(name, detail)] = _size(_tool_data(mcp, "describe_table", {"table": name, "detail": detail}))
    assert {key: size for key, size in sizes.items() if size > server.REPLY_CHARS - LIVE_ROOM} == {}


# L7: lookup_agency says what current_lineage covers, points at the registry's reasoning, and labels parents.

def _lookup(namespace, identifier):
    return _tool_data(server.build_server(), "lookup_agency", {"namespace": namespace, "identifier": identifier})


def test_a_parent_is_labelled_where_the_registrys_rows_name_it_and_null_where_none_does():
    assert _lookup("regulations.gov:agency", "FAA")["parent_labels"] == {
        "urn:ref:federal-register-agency:492": "Transportation Department"}
    # ACL's Federal Hierarchy parent is named by no vendored row; the bridge's Register parent is.
    assert _lookup("regulations.gov:agency", "ACL")["parent_labels"] == {
        "urn:ref:federal-hierarchy-org:100004222": None,
        "urn:ref:federal-register-agency:221": "Health and Human Services Department"}


def test_event_and_bridge_parents_are_labelled_too():
    labels = _lookup("federal_register_agency", "559")["parent_labels"]  # HCFA: an event original under HHS
    assert labels["urn:ref:federal-register-agency:221"] == "Health and Human Services Department"


def test_the_text_says_current_lineage_is_for_register_ids_and_points_at_the_reasoning():
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "lookup_agency"]
    text = " ".join((tool.description or "").split())
    assert "current_lineage is computed for federal_register_agency ids only" in text
    assert "reasoning" in text and "parent_labels" in text
    assert _lookup("regulations.gov:agency", "FNS")["registry_evidence"]["current_lineage"]["status"] == "not_applicable"


# L10 (owner decision 2026-10-03): list_sources groups tables by the dictionary's subject (implementer B's field).

def test_list_sources_groups_tables_by_subject_with_unassigned_tables_last(monkeypatch):
    metadata = {name: dict(entry) for name, entry in server._table_metadata().items()}
    for name, subject in (("dockets", "rulemaking"), ("documents", "rulemaking"), ("laws", "congress")):
        metadata[name]["subject"] = subject
    del metadata["comments"]["subject"]  # a table the dictionary gives no subject, as a local file can be
    monkeypatch.setattr(server, "_table_metadata", lambda: metadata)
    with duckdb.connect() as con:
        for name in ("laws", "documents", "dockets", "comments"):
            con.execute(f'CREATE TABLE "{name}" (id VARCHAR)')
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        listed = _tool_data(mcp, "list_sources", {})
        pages = {name: _tool_data(mcp, "list_sources", {"subject": name})
                 for name in [*(group["subject"] for group in listed["subjects"]), *listed["pages"]]}
        with pytest.raises(ToolError) as refused:
            _tool_data(mcp, "list_sources", {"subject": "rulemakng"})
        # A declared subject with no loaded table is empty, not unknown.
        spending = _tool_data(mcp, "list_sources", {"subject": "spending"})
    assert "tables" not in listed
    # The default names each subject's tables in page order; the page gives their label, coverage and rows.
    assert listed["subjects"] == [{"subject": "rulemaking", "tables": ["dockets", "documents"]},
                                  {"subject": "congress", "tables": ["laws"]},
                                  {"subject": "unassigned", "tables": ["comments"]}]
    assert spending["subjects"] == [{"subject": "spending", "tables": []}]
    assert {reply["generation"] for reply in (listed, spending, *pages.values())} == {listed["generation"]}
    assert listed["pages"] == ["views"]
    assert [(group["subject"], [row["table"] for row in group["tables"]])
            for name in ("rulemaking", "congress", "unassigned") for group in pages[name]["subjects"]] == [
        ("rulemaking", ["dockets", "documents"]), ("congress", ["laws"]), (None, ["comments"])]
    assert set(pages["rulemaking"]["subjects"][0]["tables"][0]) == {"table", "label", "coverage", "rows"}
    assert str(refused.value).startswith("Error executing tool list_sources: Unknown subject 'rulemakng'. "
                                         "list_sources takes one of: rulemaking, congress, unassigned, ")
    assert ", spending, " in str(refused.value) and str(refused.value).endswith(", views.")


def test_default_discovery_filters_the_complete_explicit_inventory(monkeypatch):
    """Explicit pages retain every table and default discovery lists the visible subset in order."""
    from tests.test_default_discovery_visibility import HIDDEN
    from tests.test_chaos_r3_server import _typed_tables

    with duckdb.connect() as con:
        metadata = server._table_metadata()
        loaded = [table for table in server.TABLES if table in metadata]
        _typed_tables(con, loaded)
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        listed = _tool_data(mcp, "list_sources", {})
        subjects = dict.fromkeys(metadata[table].get("subject") or server.UNASSIGNED_SUBJECT for table in loaded)
        if server.UNASSIGNED_SUBJECT in subjects:
            subjects[server.UNASSIGNED_SUBJECT] = subjects.pop(server.UNASSIGNED_SUBJECT)
        names = [*subjects, *listed["pages"]]
        pages = {name: _tool_data(mcp, "list_sources", {"subject": name}) for name in names}
        relationships = server._connection_relationships(con.cursor())
    groups = [group for name in names for group in pages[name].get("subjects", [])]
    visible_groups = []
    for group in groups:
        visible_names = [table["table"] for table in group["tables"] if table["table"] not in HIDDEN]
        if visible_names:
            visible_groups.append({"subject": group["subject"] or server.UNASSIGNED_SUBJECT, "tables": visible_names})
    assert listed["subjects"] == visible_groups
    # Every available table is on exactly one subject page, every relationship view on the views page.
    tables = [table["table"] for group in groups for table in group["tables"]]
    views = [name for group in pages["views"]["relationship_views"] for name in group["views"]]
    child_queries = {name for name, entry in relationships.items()
                     if entry["metadata"].get("view_role") == "child_query" and entry["status"] == "available"}
    available_views = {name for name, entry in relationships.items()
                       if entry["metadata"].get("view_role") != "child_query" and entry["status"] == "available"}
    assert set(tables) == (set(loaded) | child_queries) - available_views
    assert set(HIDDEN) <= set(tables)
    assert len(tables) == len(set(tables)) and not set(tables) & set(views)
    assert sum(len(group["views"]) for group in pages["views"]["deferred_views"]) == sum(
        group["count"] for group in listed["deferred_views"])
    assert listed["relationship_views"] == {"available": len(views)}
    assert listed["unavailable_views"] == len(pages["views"]["unavailable_views"])
    visible_unavailable = [name for name in pages["views"]["unavailable_tables"] if name not in HIDDEN]
    assert listed["unavailable_tables"] == visible_unavailable[:server.UNAVAILABLE_TABLES_LISTED]
    assert listed["unavailable_table_count"] == len(visible_unavailable)
    assert available_views <= set(views)
    assert listed["etl_receipts"]["dataset_count"] == len(metadata["etl_receipts"]["datasets"])
    assert listed["fec_release"]["status_counts"] == pages["views"]["fec_release"]["status_counts"]


def test_no_dictionary_subject_takes_a_page_name_list_sources_reserves():
    subjects = {entry.get("subject") for entry in server._table_metadata().values()}
    assert not subjects & {server.UNASSIGNED_SUBJECT, "views", "archive"}


def test_discovery_keeps_special_categories_and_descriptions_keep_exact_declarations(monkeypatch):
    metadata = {name: dict(entry) for name, entry in server._table_metadata().items()}
    categories = dict(laws="query_data", documents="diagnostics", dockets="processing_evidence", comments=None,
                      fcc_filings="bounded_samples", court_opinions="discovery_leads", bill_versions="source_evidence")
    for name, category in categories.items():
        if category is None:
            metadata[name].pop("category", None)
        else:
            metadata[name]["category"] = category
    monkeypatch.setattr(server, "_table_metadata", lambda: metadata)
    with duckdb.connect() as con:
        for name in categories:
            con.execute(f'CREATE TABLE "{name}" (id VARCHAR)')
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        listed = [_tool_data(mcp, "list_sources", {"subject": group["subject"]})
                  for group in _tool_data(mcp, "list_sources", {})["subjects"]]
        rows = {row["table"]: row for page in listed for row in page["subjects"][0]["tables"]}
        assert set(rows) == set(categories)
        assert all("query_data" in page["category_details"] and "undeclared" in page["category_details"]
                   for page in listed)
        for name, category in categories.items():
            described = _tool_data(mcp, "describe_table", {"table": name})
            assert described["metadata"].get("category") == category
            if category not in (None, "query_data"):
                assert rows[name]["category"] == category
            else:
                assert "category" not in rows[name]
            assert rows[name]["label"] == metadata[name]["label"]
            assert rows[name]["coverage"] == metadata[name]["kind"]


def test_the_query_text_says_pragmas_table_forms_count_as_select():
    """L9: a persona read PRAGMA as refused; DuckDB rewrites its table-returning forms to SELECT."""
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "query_sql"]
    assert "PRAGMA's table forms" in " ".join((tool.description or "").split())
