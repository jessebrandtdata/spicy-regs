"""One retained historical item; no inferred absence or time-of-day boundary."""

from copy import deepcopy
import json

import pytest

from spicy_regs.scorecards.action_comparisons import (
    COSPONSOR_COLUMNS, RESULT_COLUMNS, SPONSORSHIP_BILL, SPONSORSHIP_ITEM, SPONSORSHIP_SCOPE,
    compare_historical_sponsorship, selected_native_rows, sponsorship_method,
)
from tests.test_scorecard_action_comparisons import fixture
from tests.test_scorecard_native_main_inputs import native_selection

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def historical_fixture():
    context = dict(scorecard_id=SPONSORSHIP_SCOPE, snapshot_id="s1", capture_id="c1",
                   source_url="https://publisher.example/report.pdf", source_path="pdf:page[4];/methodology")
    source = {"scorecards": [context | dict(publisher_id="common_cause", year_text="2022", congress_text="117")],
              "scorecard_items": [context | dict(item_id=SPONSORSHIP_ITEM, item_kind_text="BILLS TO COSPONSOR",
                  publisher_position_text="BILLS TO COSPONSOR", congress_text="117", chamber_text="House",
                  bill_citation_text="HR 1334", title="HR 1334, DISCLOSE Act")]}
    methodologies = [context | dict(methodology_id="House:methodology", chamber_text="House",
        methodology_text=json.dumps({"source_cutoff_date_text": "September 6, 2022",
            "source_methodology_text": "All cosponsors listed in this Democracy Scorecard were taken from the "
                                       "Congress.gov website as of September\n6, 2022."}))]
    links = {"scorecard_member_links": [], "scorecard_item_links": [dict(scorecard_id=SPONSORSHIP_SCOPE,
        item_id=SPONSORSHIP_ITEM, source_snapshot_id="s1", reference_id=ref, resolution_status="resolved",
        bill_id=SPONSORSHIP_BILL, vote_id=None, amendment_id=None)
        for ref in ("item:direct", "reference:header-citation-0")]}
    results, cosponsors = [], []
    # Exact retained date/credit facts: Norcross before cutoff and Mfume after.
    for index, (bio, credit, start) in enumerate((
        ("N000188", "✔", "2022-08-26"), ("M000687", "✖", "2022-09-28"),
    )):
        results.append(dict.fromkeys(RESULT_COLUMNS) | context | dict(item_id=SPONSORSHIP_ITEM,
            publisher_member_key=bio, result_id="printed-cell", result_text=credit))
        links["scorecard_member_links"].append(dict(scorecard_id=SPONSORSHIP_SCOPE, publisher_member_key=bio,
            source_snapshot_id="s1", resolution_status="resolved", bioguide_id=bio))
        cosponsors.append(dict(bill_id=SPONSORSHIP_BILL, cosponsor_index=index, bioguide_id=bio,
                               sponsorship_date=start, sponsorship_withdrawn_date=None))
    pins = fixture()[5]
    pins.update({name: deepcopy(pins["member_votes"]) for name in ("scorecard_methodologies", "bill_cosponsors")})
    return source, links, results, cosponsors, pins, methodologies


def compare(inputs):
    source, links, results, cosponsors, pins, methodologies = inputs
    return compare_historical_sponsorship(source, links, results, cosponsors, pins,
                                          sponsorship_method(source, methodologies))


def test_retained_before_after_dates_preserve_credit_and_all_references():
    inputs = historical_fixture()
    original = deepcopy(inputs)
    rows = compare(inputs)
    assert [r["comparison_status"] for r in rows] == ["same", "different"]
    assert [r["publisher_credit_text"] for r in rows] == ["✔", "✖"]
    assert [r["official_position_normalized"] for r in rows] == ["cosponsor_at_cutoff", "not_cosponsor_at_cutoff"]
    assert all(r["official_position_text"] is None and r["vote_id"] is None for r in rows)
    assert rows[1]["official_member_key"] == "117-hr-1334/cosponsor_index:1"
    assert inputs == original


@pytest.mark.parametrize("start,end,status,reason", [
    ("2022-09-06", None, "unresolved", "same_day_cutoff_time_unknown"),
    ("2022-08-26", "2022-09-06", "unresolved", "same_day_cutoff_time_unknown"),
    ("2022-08-26", "2022-09-05", "different", "dated_official_cosponsorship_at_source_cutoff"),
    ("2022-08-26", "2022-09-07", "same", "dated_official_cosponsorship_at_source_cutoff"),
    (None, None, "unresolved", "missing_or_invalid_official_sponsorship_dates"),
    ("2022-08-26", "", "unresolved", "missing_or_invalid_official_sponsorship_dates"),
    ("2022-08-26", "2022-08-25", "unresolved", "missing_or_invalid_official_sponsorship_dates"),
    ("2022-02-30", None, "unresolved", "missing_or_invalid_official_sponsorship_dates"),
])
def test_date_boundaries(start, end, status, reason):
    inputs = historical_fixture()
    inputs[3][0].update(sponsorship_date=start, sponsorship_withdrawn_date=end)
    row = compare(inputs)[0]
    assert (row["comparison_status"], row["reason"]) == (status, reason)


@pytest.mark.parametrize("change", ["missing", "duplicate", "member", "ref_missing", "ref_other_bill", "ref_stale", "ref_roll"])
def test_missing_or_conflicting_evidence_never_becomes_absence(change):
    inputs = historical_fixture()
    if change == "missing":
        inputs[3].pop(0)
    elif change == "duplicate":
        inputs[3].append(inputs[3][0].copy())
    elif change == "member":
        inputs[1]["scorecard_member_links"][0]["resolution_status"] = "unresolved"
    elif change == "ref_missing":
        inputs[1]["scorecard_item_links"].pop()
    else:
        key, value = {"ref_other_bill": ("bill_id", "117-hr-1"), "ref_stale": ("source_snapshot_id", "old"),
                      "ref_roll": ("vote_id", "117-house-1-1")}[change]
        inputs[1]["scorecard_item_links"][1][key] = value
    assert compare(inputs)[0]["comparison_status"] == "unresolved"


@pytest.mark.parametrize("change", ["missing", "changed", "unsupported_statement", "stale", "item"])
def test_missing_or_changed_source_method_refuses_qualification(change):
    inputs = historical_fixture()
    if change == "missing":
        inputs[5].clear()
    elif change == "stale":
        inputs[5][0]["snapshot_id"] = "old"
    elif change == "item":
        inputs[0]["scorecard_items"][0]["bill_citation_text"] = "HR 1"
    else:
        text = json.loads(inputs[5][0]["methodology_text"])
        text["source_cutoff_date_text" if change == "changed" else "source_methodology_text"] = "September 7, 2022"
        inputs[5][0]["methodology_text"] = json.dumps(text)
    with pytest.raises(ValueError, match="Historical sponsorship"):
        compare(inputs)


def test_native_cosponsor_filter_preserves_dates_and_key(tmp_path):
    index, subject, _ = native_selection(tmp_path, "bill_cosponsors", [
        dict(bill_id=SPONSORSHIP_BILL, cosponsor_index="0", bioguide_id="N000188", sponsorship_date="2022-08-26"),
        dict(bill_id="117-hr-1", cosponsor_index="0", bioguide_id="N000188", sponsorship_date="2021-01-01"),
    ])
    rows = selected_native_rows(index, "bill_cosponsors", [subject], COSPONSOR_COLUMNS, "bill_id", {SPONSORSHIP_BILL})
    assert len(rows) == 1 and rows[0]["sponsorship_date"] == "2022-08-26" and rows[0]["cosponsor_index"] == 0


def test_selected_historical_method_flows_through_native_analysis(tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
    from tests.test_scorecard_analysis import _inputs, _serve

    source, _, results, cosponsors, _, methodologies = historical_fixture()
    item = source["scorecard_items"][0]
    item["references_json"] = json.dumps([dict(occurrence_id="header-citation-0", citation_text="HR 1334", kind="bill", source_path="/header",
                               congress_text="117", chamber_text="House", bill_citation_text="HR 1334")])
    context = {k: item[k] for k in ("scorecard_id", "snapshot_id", "capture_id", "source_url", "source_path")}
    store, index = _inputs(tmp_path, row_overrides={
        **source, "scorecard_methodologies": methodologies,
        "scorecard_members": [context | dict(publisher_member_key="N000188", member_name="Donald Norcross",
            bioguide_id="N000188", state="NJ", district="1", chamber_text="House")],
        "scorecard_member_item_results": results[:1],
        "members": [dict(bioguide_id="N000188", name_first="Donald", name_last="Norcross")],
        "member_terms": [dict(bioguide_id="N000188", term_index="0", term_type="rep", term_start="2021-01-03",
                              term_end="2023-01-03", term_state="NJ", term_district="1")],
        "congress_bills": [dict(bill_id=SPONSORSHIP_BILL, congress="117", bill_type="hr", bill_number="1334")],
        "bill_cosponsors": [cosponsors[0] | {"cosponsor_index": "0"}],
    })
    _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    _, links, comparisons = rollup.build(output)
    rows = pq.read_table(comparisons).to_pylist()
    assert len(rows) == 1 and rows[0]["comparison_status"] == "same"
    assert pq.read_table(links).num_rows == 2
    receipt = json.loads((output / "scorecard-analysis-qualification.json").read_text())
    assert receipt["comparison"]["historical_sponsorship"]["cutoff_date"] == "2022-09-06"
    assert set(receipt["comparison"]["input_representations"]) >= {"bill_cosponsors", "scorecard_methodologies"}
