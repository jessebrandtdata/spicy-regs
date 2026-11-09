"""Pins rollup freshness checks: date watermarks, row-count budgets, and refusal of a damaged managed member.

A failed read must not move the recorded history.
"""

from datetime import date
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import check_rollup_freshness as freshness
from scripts.check_rollup_freshness import evaluate_comment_coverage, evaluate_date_rows, evaluate_row_changes
from spicy_regs.sources import publication


def test_all_published_tables_receive_integrity_checks(tmp_path, monkeypatch):
    from contextlib import nullcontext

    monkeypatch.setattr(freshness, 'DATE_CHECKS', ())
    monkeypatch.setattr(freshness, 'ROW_CHANGE_BUDGETS', {})
    member = tmp_path / 'generation' / 'new_family.parquet'
    member.parent.mkdir()
    pq.write_table(pa.table({'id': ['one']}), member)
    index = {'families': {'new_family': {
        'prefix': 'generation', 'artifactDigest': 'sha256:observed',
        'tables': {'new_family.parquet': {'columns': [['id', 'VARCHAR']], 'rows': 1, 'sha256': 'sha256:' + '0' * 64,
                                          'byteSize': member.stat().st_size}}
    }}}
    monkeypatch.setattr(publication, 'snapshot', lambda _: nullcontext(index))
    assert freshness.read_freshness_rows(str(tmp_path)) == [('new_family', 'publication rows', None, 1)]
    index['families']['new_family']['tables']['new_family.parquet']['rows'] = 2
    with pytest.raises(publication.PublicationError, match='row count'):
        freshness.read_freshness_rows(str(tmp_path))


def test_date_freshness_checks_both_congress_watermarks():
    rows = [
        ("congress_bills", "update watermark", "2026-07-19", 100),
        ("congress_bills", "latest action", "2025-04-02", 100),
    ]
    failures = evaluate_date_rows(rows, date(2026, 7, 20))
    assert len(failures) == 1
    assert "latest action" in failures[0]


def test_row_count_state_flags_a_stalled_source():
    state = {"usaspending_recipients": {"count": 100_000, "last_changed": "2026-07-01"}}
    rows = [("usaspending_recipients", "row count", None, 100_000)]
    failures = evaluate_row_changes(rows, state, date(2026, 7, 20))
    assert failures


def test_row_count_change_resets_the_clock():
    state = {"usaspending_recipients": {"count": 99_000, "last_changed": "2026-07-01"}}
    rows = [("usaspending_recipients", "row count", None, 100_000)]
    assert evaluate_row_changes(rows, state, date(2026, 7, 20)) == []
    assert state["usaspending_recipients"] == {"count": 100_000, "last_changed": "2026-07-20"}


def test_base_tables_are_monitored():
    """dockets/documents must stay covered — nothing watched them for 8 weeks."""
    from scripts.check_rollup_freshness import DATE_CHECKS

    monitored = {(c.table, c.column) for c in DATE_CHECKS}
    assert ("dockets", "modify_date") in monitored
    assert ("documents", "modify_date") in monitored


def test_base_table_checks_avoid_future_dated_posted_date():
    """documents.posted_date carries future effective dates and can't detect a stall."""
    from scripts.check_rollup_freshness import DATE_CHECKS

    assert ("documents", "posted_date") not in {(c.table, c.column) for c in DATE_CHECKS}


def test_frozen_dockets_watermark_fails():
    """The exact production regression: dockets stuck at 2026-07-02."""
    rows = [("dockets", "modify_date", "2026-07-02T20:46:17Z", 276_326)]
    failures = evaluate_date_rows(rows, date(2026, 8, 26))
    assert len(failures) == 1
    assert "dockets" in failures[0]


def test_current_dockets_watermark_passes():
    rows = [("dockets", "modify_date", "2026-08-25T12:00:00Z", 278_000)]
    assert evaluate_date_rows(rows, date(2026, 8, 26)) == []


@pytest.fixture
def selected_files(tmp_path, monkeypatch):
    """Real Parquet reads, with a local directory standing in for HTTP storage."""
    root = tmp_path / "publisher's-data"
    root.mkdir()
    monkeypatch.setattr(
        freshness,
        "DATE_CHECKS",
        (
            freshness.DateCheck("congress_bills", "update_date", 7),
            freshness.DateCheck("congress_bills", "latest_action_date", 7),
            freshness.DateCheck("dockets", "modify_date", 4),
        ),
    )
    monkeypatch.setattr(freshness, "ROW_CHANGE_BUDGETS", {"usaspending_recipients": 14})
    pq.write_table(
        pa.table({"update_date": ["2000-01-01"], "latest_action_date": ["2000-01-01"]}), root / "congress_bills.parquet"
    )
    pq.write_table(pa.table({"modify_date": ["2026-09-21"]}), root / "dockets.parquet")
    pq.write_table(pa.table({"id": ["one", "two"]}), root / "usaspending_recipients.parquet")
    index = publication.empty_index()
    prefix = "generations/bill-family/" + "a" * 64
    member = root / prefix / "congress_bills.parquet"
    member.parent.mkdir(parents=True)
    pq.write_table(pa.table({"update_date": ["2026-09-21"], "latest_action_date": ["2026-09-20"]}), member)
    index["families"]["bill-family"] = {
        "prefix": prefix,
        "logicalId": "urn:test:bill-family",
        "artifactDigest": "sha256:" + "a" * 64,
        "tables": {
            "congress_bills.parquet": {
                "sha256": "sha256:" + "b" * 64,
                "byteSize": member.stat().st_size,
                "rows": 1,
                "columns": [["update_date", "VARCHAR"], ["latest_action_date", "VARCHAR"]],
            }
        },
    }
    calls = []

    def load(base_url):
        calls.append(base_url)
        return publication.parse_index(json.dumps(index).encode())

    monkeypatch.setattr(publication, "load_index", load)
    return root, member, index, calls


def test_freshness_uses_one_managed_snapshot_and_legacy_unowned_files(selected_files, capsys):
    root, _member, _index, calls = selected_files
    rows = freshness.read_freshness_rows(str(root))
    assert rows == [
        ("congress_bills", "update_date", "2026-09-21 00:00:00", 1),
        ("congress_bills", "latest_action_date", "2026-09-20 00:00:00", 1),
        ("dockets", "modify_date", "2026-09-21 00:00:00", 1),
        ("usaspending_recipients", "row count", None, 2),
    ]
    assert calls == [str(root)]
    output = capsys.readouterr().out
    assert "managed artifact=sha256:" in output
    assert "byte digest not rechecked" in output
    assert "dockets legacy" in output


def test_freshness_keeps_legacy_resolution_when_index_is_absent(selected_files):
    root, _member, index, calls = selected_files
    index["families"].clear()
    rows = freshness.read_freshness_rows(str(root))
    assert rows[0][2] == "2000-01-01 00:00:00"
    assert calls == [str(root)]


def test_selected_table_does_not_read_unrelated_managed_files(selected_files):
    root, _member, index, calls = selected_files
    index["families"]["unrelated"] = {
        **index["families"]["bill-family"],
        "prefix": "generations/unrelated/" + "a" * 64,
        "tables": {"missing.parquet": {"columns": [["id", "VARCHAR"]], "rows": 1,
                                      "sha256": "sha256:" + "c" * 64, "byteSize": 100}},
    }
    assert freshness.read_freshness_rows(str(root), ["congress_bills"]) == [
        ("congress_bills", "update_date", "2026-09-21 00:00:00", 1),
        ("congress_bills", "latest_action_date", "2026-09-20 00:00:00", 1),
    ]
    assert calls == [str(root)]


@pytest.mark.parametrize("latest,expected", [("2026-11-03", 0), ("2026-11-02", 1), (None, 1)])
def test_lobbying_cli_alerts_on_old_or_missing_newest_posting(tmp_path, monkeypatch, capsys, latest, expected):
    from contextlib import nullcontext

    pq.write_table(pa.table({"dt_posted": pa.array([latest], type=pa.string())}), tmp_path / "lobbying_filings.parquet")
    monkeypatch.setattr(publication, "snapshot", lambda _: nullcontext(publication.empty_index()))
    monkeypatch.setattr(freshness, "resolve_r2_base_url", lambda value: str(tmp_path))

    def unrelated_comment_read(_):
        pytest.fail("a single-table freshness check must not read comment coverage")

    monkeypatch.setattr(freshness, "read_comment_counts", unrelated_comment_read)
    monkeypatch.setattr("sys.argv", ["check-freshness", "--table", "lobbying_filings", "--today", "2026-11-06",
                                    "--state-file", str(tmp_path / "history.json")])
    assert freshness.main() == expected
    output = capsys.readouterr().out
    assert "lobbying_filings" in output
    assert ("Freshness failures:" in output) == bool(expected)


@pytest.mark.parametrize("damage", ["missing", "malformed", "schema", "row_count"])
def test_broken_managed_member_refuses_instead_of_reading_bare_fallback(selected_files, damage):
    root, member, index, _calls = selected_files
    if damage == "missing":
        member.unlink()
    elif damage == "malformed":
        member.write_bytes(b"this is not parquet")
    elif damage == "schema":
        index["families"]["bill-family"]["tables"]["congress_bills.parquet"]["columns"][0][1] = "DATE"
    else:
        index["families"]["bill-family"]["tables"]["congress_bills.parquet"]["rows"] = 2
    assert (root / "congress_bills.parquet").exists()
    with pytest.raises((duckdb.Error, publication.PublicationError)):
        freshness.read_freshness_rows(str(root))


def test_invalid_publication_refuses_legacy_fallback(selected_files):
    root, _member, index, _calls = selected_files
    index["families"]["bill-family"]["artifactDigest"] = "invalid"
    with pytest.raises(publication.PublicationError, match="Invalid publication index"):
        freshness.read_freshness_rows(str(root))


def test_failed_freshness_read_does_not_change_row_history(selected_files, tmp_path, monkeypatch):
    root, member, _index, _calls = selected_files
    member.unlink()
    state = tmp_path / "history.json"
    original = '{"usaspending_recipients":{"count":7,"last_changed":"2026-09-01"}}'
    state.write_text(original)
    monkeypatch.setattr(freshness, "resolve_r2_base_url", lambda value: str(root))
    monkeypatch.setattr("sys.argv", ["check-freshness", "--state-file", str(state)])
    assert freshness.main() == 1
    assert state.read_text() == original


def _baseline(counts):
    return {"comments_coverage": {"agencies": dict(counts), "as_of": "2026-09-05"}}


def test_comment_coverage_first_run_sets_baseline():
    state = {}
    assert evaluate_comment_coverage({"EPA": 100, "VA": 386_000}, state, date(2026, 9, 5)) == []
    assert state["comments_coverage"]["agencies"] == {"EPA": 100, "VA": 386_000}


def test_comment_coverage_flags_the_197_failure():
    """103 of 179 agencies left: the vanished agencies must fail, and keep failing."""
    state = _baseline({"EPA": 1_000_000, "VA": 386_000, "ERULE": 1})
    failures = evaluate_comment_coverage({"EPA": 1_000_000}, state, date(2026, 9, 7))
    assert any("VA vanished" in f for f in failures)
    assert any("ERULE vanished" in f for f in failures)
    assert any("total fell" in f for f in failures)
    # Baseline untouched, so the next night fails again instead of going green.
    assert state["comments_coverage"]["agencies"]["VA"] == 386_000
    assert evaluate_comment_coverage({"EPA": 1_000_000}, state, date(2026, 9, 8))


def test_comment_coverage_flags_a_stub_agency():
    state = _baseline({"EPA": 25_000_000, "VA": 386_000})
    failures = evaluate_comment_coverage({"EPA": 25_000_000, "VA": 12}, state, date(2026, 9, 9))
    # 386k of 25.4M is under the 2% total budget, so only the per-agency check fires.
    assert failures == ["comments: agency VA fell 386,000 -> 12 rows"]


def test_comment_coverage_tolerates_growth_and_dedupe_trickle():
    state = _baseline({"EPA": 1_000_000, "OMB": 800})
    counts = {"EPA": 999_600, "OMB": 300, "NEW": 5}
    assert evaluate_comment_coverage(counts, state, date(2026, 9, 9)) == []
    assert state["comments_coverage"]["agencies"] == counts


def test_comment_coverage_accept_rebaselines():
    state = _baseline({"EPA": 1_000, "GONE": 5_000})
    assert evaluate_comment_coverage({"EPA": 1_000}, state, date(2026, 9, 9), accept=True) == []
    assert state["comments_coverage"]["agencies"] == {"EPA": 1_000}


def test_comment_coverage_reads_per_agency_rows_from_the_published_index(tmp_path):
    pq.write_table(pa.table({"agency_code": ["VA", "VA", "EPA", None], "row_count": [10, 5, 7, 3]}),
                   tmp_path / "comments_index.parquet")
    assert freshness.read_comment_counts(str(tmp_path)) == {"VA": 15, "EPA": 7}


def test_senate_vote_freshness_is_held_apart_from_the_house(tmp_path):
    """A fresh House max must not hide a stale Senate half (ss-bruk); the row count stays the whole table's pin."""
    member = tmp_path / "roll_call_votes.parquet"
    pq.write_table(pa.table({"chamber": ["house", "senate", "senate", "house"],
                             "vote_day": ["2026-11-06", "2026-08-01", None, "2026-11-05"]}), member)  # fmt: skip
    check = next(c for c in freshness.DATE_CHECKS if c.table == "roll_call_votes")
    rows = duckdb.sql(freshness._query({"roll_call_votes": [str(member)]})).fetchall()
    assert [(table, metric, latest[:10], count) for table, metric, latest, count in rows] == [
        ("roll_call_votes", "senate vote day", "2026-08-01", 4)
    ]
    assert check.budget_days == 60
    assert evaluate_date_rows(rows, date(2026, 11, 9)), "100 days without a Senate vote is stale"
    # The election-year October recess (2024-09-25 to 2024-11-12, 48 days) is not.
    assert evaluate_date_rows([("roll_call_votes", "senate vote day", "2024-09-25", 1)], date(2024, 11, 11)) == []
