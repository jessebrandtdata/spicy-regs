"""Reviewed Senate capture survives consumer replay without changing source scope."""

import gzip
import json
from collections import Counter
from pathlib import Path

import pytest

from spicy_regs.pipelines.congressional_disbursement_pages import pin, processing_inputs
from spicy_regs.pipelines.disbursement_page_profiles import read_retained_observation
from spicy_regs.transforms.disbursement_pdf_tables import pages
from tests.disbursement_capture_fixtures import selected_capture

FIXTURE = Path(__file__).parent / "fixtures/senate_office_observations"


def entry_for(page, path):
    with gzip.open(path, "wt") as stream:
        stream.write(json.dumps(page) + "\n")
    return {
        "output": pin(path),
        "selected_pages": [20],
        "physical_pages": 1069,
        "pages": 1,
        "records": 7,
        "repairs": 0,
        "reading_profile": "financial-summary",
        "page_statuses": {page["status"]: 1},
        "row_kinds": dict(Counter(row["row_kind"] for row in page["records"])),
    }


def test_actual_flex_capture_preserves_blanks_and_separate_funding_year(tmp_path):
    source, item = selected_capture(FIXTURE, tmp_path)
    assert processing_inputs(source)
    page = read_retained_observation(item, chamber="senate", profile="financial-summary")
    entry = entry_for(page, tmp_path / "pages.jsonl.gz")
    assert list(pages(source, entry)) == [page]
    assert page["printed_page"] == "B-1"
    assert page["context"]["funding_year"] == "2006"
    assert page["context"]["account_label"] is None
    assert all((row["period_start"], row["period_end"]) == ("2008-04-01", "2008-09-30") for row in page["records"])
    values = [value for row in page["records"] for value in row["values"].values()]
    assert len(values) == 21 and values.count(None) == 13 and values.count("0.00") == 6
    assert page["records"][3]["values"]["net_funds_available"] == "-5000.00"
    assert page["records"][4]["label"] == "Land and Structures"
    assert all(value is None for value in page["records"][4]["values"].values())
    assert page["empty_ledgers"][0]["status"] == "observed_empty"
    assert page["issues"] == ["text_not_fully_source_qualified"]


@pytest.mark.parametrize("change", ["original", "period", "chamber", "page", "plan"])
def test_captured_summary_refuses_other_source_or_capture_scope(tmp_path, change):
    source, item = selected_capture(FIXTURE, tmp_path)
    if change == "original":
        source["source_sha256"] = "sha256:" + "0" * 64
    elif change == "period":
        source["period_start"] = "2006-04-01"
    elif change == "chamber":
        source["chamber"] = "house"
    elif change == "page":
        item["page"] = 21
    else:
        item["capture_binding"]["plan"]["sha256"] = "sha256:" + "0" * 64
    with pytest.raises(ValueError):
        processing_inputs(source)


@pytest.mark.parametrize("change", ["blank_to_zero", "funding_year", "printed_page", "ledger"])
def test_rehashed_page_cannot_change_reviewed_facts_or_empty_observation(tmp_path, change):
    source, item = selected_capture(FIXTURE, tmp_path)
    page = read_retained_observation(item, chamber="senate", profile="financial-summary")
    if change == "blank_to_zero":
        page["records"][4]["values"]["net_funds_available"] = "0.00"
    elif change == "funding_year":
        page["context"]["funding_year"] = "2008"
    elif change == "printed_page":
        page["printed_page"] = "51"
    else:
        page["empty_ledgers"] = []
    entry = entry_for(page, tmp_path / "changed.jsonl.gz")
    with pytest.raises(ValueError, match="differs from its retained observation"):
        list(pages(source, entry))
