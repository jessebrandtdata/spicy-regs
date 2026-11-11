"""Narrow LCV method: preserve credit and refuse unsupported evidence."""
from copy import deepcopy
import json

import pytest

from spicy_regs.scorecards.action_comparisons import (
    COLUMNS, RESULT_COLUMNS, TABLE, VOTE_COLUMNS, compare_lcv_2025, selected_native_rows,
)
from spicy_regs.scorecards.etl import ANALYSIS_NAMES, SOURCE_NAMES, generation_options, read_family, write_family
from spicy_regs.scorecards.resolution import INPUT_TABLES
from spicy_regs.generations import build_generation
from tests.test_scorecard_native_main_inputs import native_selection

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def fixture():
    source = {"scorecards": [{"scorecard_id": "lcv:2025", "snapshot_id": "s1", "publisher_id": "lcv", "year_text": "2025"}],
              "scorecard_items": []}
    links = {"scorecard_member_links": [], "scorecard_item_links": []}
    results, votes, people, metrics = [], [], [], []
    cases = ["same", "different", "not_voting", "present", "missing", "not_applicable", "target",
             "ambiguous", "repeated", "stale", "duplicate_vote", "unknown_position"]
    for number, case in enumerate(cases):
        item, bio = str(number), f"X{number:06}"
        source["scorecard_items"].append(dict(scorecard_id="lcv:2025", snapshot_id="s1", item_id=item,
            item_kind_text="cosponsorship" if case == "not_applicable" else "Scorecard Vote",
            publisher_position_text="Oppose" if case == "target" else "No"))
        results.append(dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="lcv:2025", snapshot_id="s1", item_id=item,
            result_id="reported", publisher_member_key=bio, metric_id="annual", participation_id="scorecard", result_text="excused",
            capture_id="capture", source_url="https://publisher.example", source_path="CSV/row[1]/Vote Type"))
        metrics.append(dict(scorecard_id="lcv:2025", snapshot_id="s1", metric_id="annual", item_id=item,
                            participation_id="scorecard", publisher_position_text="No"))
        links["scorecard_member_links"].append(dict(scorecard_id="lcv:2025", publisher_member_key=bio,
            source_snapshot_id="s1", bioguide_id=bio, resolution_status="resolved"))
        link: dict[str, str | None] = dict(scorecard_id="lcv:2025", item_id=item, source_snapshot_id="old" if case == "stale" else "s1",
                    reference_id="item:direct", vote_id="119-senate-1-24", resolution_status="resolved")
        if case == "ambiguous":
            link.update(resolution_status="ambiguous", vote_id=None)
        links["scorecard_item_links"].append(link)
        if case == "repeated":
            links["scorecard_item_links"].append(link | {"reference_id": "reference:second"})
        people.append(dict(bioguide_id=bio, lis_id="S" + item))
        if case != "missing":
            position = {"same": "nay", "different": "yea", "not_voting": "not_voting", "present": "present",
                        "unknown_position": "unknown"}.get(case, "nay")
            vote = dict(vote_id="119-senate-1-24", member_key="lis:S" + item, bioguide_id=None,
                        lis_id="S" + item, position=position, position_normalized=position)
            votes.append(vote)
            if case == "duplicate_vote":
                votes.append(vote | {"member_key": "duplicate"})
    pins = {name: dict(family="scorecards" if name.startswith("scorecard") else "official",
                      artifactDigest="sha256:" + "a" * 64, sha256="sha256:" + "b" * 64, byteSize=1)
            for name in (*INPUT_TABLES, "scorecard_member_item_results", "scorecard_metric_items", "member_votes")}
    return source, links, results, votes, people, pins, metrics


def test_six_statuses_fail_closed_and_native_credit_is_unchanged(tmp_path):
    inputs = fixture()
    original = deepcopy(inputs)
    rows = compare_lcv_2025(*inputs)
    assert [r["comparison_status"] for r in rows] == [
        "same", "different", "not_voting", "present", "unresolved", "not_applicable",
        "unresolved", "unresolved", "unresolved", "unresolved", "unresolved", "unresolved"]
    assert rows[2]["publisher_credit_text"] == "excused"
    assert rows[2]["official_position_normalized"] == "not_voting"
    assert rows[6]["publisher_target_text"] == "Oppose"
    assert rows[8]["vote_id"] is None
    assert rows[9]["reason"] == "link_snapshot_mismatch"
    assert inputs == original
    assert TABLE in ANALYSIS_NAMES and TABLE not in SOURCE_NAMES
    directory = tmp_path / "comparison"
    files = write_family(directory, {TABLE: rows})
    assert read_family(directory, [TABLE])[TABLE] == rows
    build_generation(tmp_path / "generation", family="scorecard-analysis", files=files,
                     expected_keys=[TABLE + ".parquet"], **generation_options(directory, [TABLE]))


def test_changed_source_snapshot_refuses():
    inputs = fixture()
    inputs[2][0]["snapshot_id"] = "old"
    with pytest.raises(ValueError, match="selected snapshot"):
        compare_lcv_2025(*inputs)


def test_metric_specific_target_and_lis_conflicts_stay_unresolved():
    inputs = fixture()
    inputs[6][0]["publisher_position_text"] = "Yes"
    assert compare_lcv_2025(*inputs)[0]["reason"] == "missing_or_conflicting_metric_target"
    inputs = fixture()
    inputs[4].append(dict(bioguide_id="other", lis_id="S0"))
    assert compare_lcv_2025(*inputs)[0]["comparison_status"] == "unresolved"


def test_native_vote_filter_keeps_only_requested_roll_call(tmp_path):
    index, subject, _ = native_selection(tmp_path, "member_votes", [
        {"vote_id": "119-senate-1-24", "member_key": "lis:S1", "position": "Nay", "position_normalized": "nay"},
        {"vote_id": "118-house-1-1", "member_key": "X000001", "position": "Yea", "position_normalized": "yea"},
    ])
    rows = selected_native_rows(index, "member_votes", [subject], VOTE_COLUMNS, "vote_id", {"119-senate-1-24"})
    assert len(rows) == 1 and rows[0]["member_key"] == "lis:S1"
    assert selected_native_rows(index, "member_votes", [subject], VOTE_COLUMNS, "vote_id", set()) == []


def test_retained_zeldin_item_literal_examples():
    # Independently selected source51536 / analysis25d74 / official f26f rows:
    # item36294, CSV member-result cells. No publisher body is copied here.
    source, links, _, _, _, pins, _ = fixture()
    snapshot = "scorecard-snapshot:22a84305-68e5-4a7b-bc3d-1e1a49a504c5"
    source["scorecards"][0]["snapshot_id"] = snapshot
    source["scorecard_items"] = [dict(scorecard_id="lcv:2025", snapshot_id=snapshot, item_id="36294",
        item_kind_text="Scorecard Vote", publisher_position_text="No")]
    links = {"scorecard_member_links": [], "scorecard_item_links": [dict(scorecard_id="lcv:2025", item_id="36294",
        source_snapshot_id=snapshot, reference_id="item:direct", resolution_status="resolved", vote_id="119-senate-1-24")]}
    results, votes, people, metrics = [], [], [], []
    for key, bio, lis, credit, position, normalized in [
        ("angela-alsobrooks", "A000382", "S428", "pro", "Nay", "nay"),
        ("lisa-a-murkowski", "M001153", "S288", "anti", "Yea", "yea"),
        ("cory-anthony-booker", "B001288", "S370", "excused", "Not Voting", "not_voting"),
    ]:
        results.append(dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="lcv:2025", result_id="reported",
            snapshot_id=snapshot, item_id="36294", publisher_member_key=key, metric_id="annual", participation_id="scorecard", result_text=credit))
        links["scorecard_member_links"].append(dict(scorecard_id="lcv:2025", publisher_member_key=key,
            source_snapshot_id=snapshot, resolution_status="resolved", bioguide_id=bio))
        people.append(dict(bioguide_id=bio, lis_id=lis))
        votes.append(dict(vote_id="119-senate-1-24", member_key="lis:" + lis, bioguide_id=bio, lis_id=lis,
                          position=position, position_normalized=normalized))
    metrics = [dict(scorecard_id="lcv:2025", snapshot_id=snapshot, metric_id="annual", item_id="36294",
                    participation_id="scorecard", publisher_position_text="No")]
    rows = compare_lcv_2025(source, links, results, votes, people, pins, metrics)
    assert [r["comparison_status"] for r in rows] == ["same", "different", "not_voting"]
    assert [r["publisher_credit_text"] for r in rows] == ["pro", "anti", "excused"]
    assert all(tuple(r) == COLUMNS and json.loads(r["input_pins_json"]) == pins for r in rows)
