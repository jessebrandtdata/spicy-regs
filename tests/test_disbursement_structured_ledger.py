"""Retained House capture and review bindings survive the actual typed-page guard."""

import gzip
import json
from pathlib import Path

import pytest

from spicy_regs.pipelines.congressional_disbursement_pages import pin, processing_inputs
from spicy_regs.pipelines.disbursement_page_profiles import read_retained_observation
from spicy_regs.transforms.disbursement_pdf_tables import pages
from tests.disbursement_capture_fixtures import selected_capture

FIXTURE = Path(__file__).parent / "fixtures/house_disbursement_observations"


def selected(tmp_path):
    return selected_capture(FIXTURE, tmp_path)


def entry_for(page, path):
    from collections import Counter

    with gzip.open(path, "wt") as stream:
        stream.write(json.dumps(page) + "\n")
    return {
        "output": pin(path),
        "selected_pages": [49],
        "physical_pages": 1252,
        "pages": 1,
        "records": 43,
        "repairs": 0,
        "page_statuses": {page["status"]: 1},
        "row_kinds": dict(Counter(row["row_kind"] for row in page["records"])),
    }


def test_real_capture_replays_and_preserves_repeated_occurrences(tmp_path):
    source, item = selected(tmp_path)
    assert processing_inputs(source)
    page = read_retained_observation(item, chamber="house", profile="ledger")
    entry = entry_for(page, tmp_path / "pages.jsonl.gz")
    assert list(pages(source, entry)) == [page]
    repeated = [row for row in page["records"] if row["amount"] == "249.00"]
    assert len(repeated) == 2
    assert repeated[0]["source_origin"] != repeated[1]["source_origin"]
    assert all(row["line"] is row["word_indices"] is None for row in page["records"])


@pytest.mark.parametrize("change", ["original", "period", "page", "plan", "review_file"])
def test_processing_closure_refuses_other_sources_periods_or_evidence(tmp_path, change):
    source, item = selected(tmp_path)
    if change == "original":
        source["source_sha256"] = "sha256:" + "0" * 64
    elif change == "period":
        source["period_start"] = "2009-01-01"
    elif change == "page":
        item["page"] = 48
    elif change == "plan":
        item["capture_binding"]["plan"]["sha256"] = "sha256:" + "0" * 64
    else:
        qualification = json.loads(Path(item["qualification"]["path"]).read_text())
        wrong = tmp_path / "wrong-review.json"
        wrong.write_text("{}")
        qualification["review_evidence"][0]["path"] = str(wrong)
        path = Path(item["qualification"]["path"])
        path.write_text(json.dumps(qualification))
        item["qualification"] = pin(path)
    with pytest.raises(ValueError):
        processing_inputs(source)


@pytest.mark.parametrize(
    "field,value", [("amount", "511.51"), ("document", "0803100006"), ("organization", "PERSONNEL")]
)
def test_rehashed_page_cannot_change_money_voucher_or_context(tmp_path, field, value):
    source, item = selected(tmp_path)
    page = read_retained_observation(item, chamber="house", profile="ledger")
    page["records"][8][field] = value
    entry = entry_for(page, tmp_path / "changed-pages.jsonl.gz")
    with pytest.raises(ValueError, match="differs from its retained observation"):
        list(pages(source, entry))
